import json
import random
import unittest
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from ..adapters.file_output import write_file
from ..adapters.repository import ensure_merchants
from ..domain.events import make_event
from ..domain.fraud import fraud_overrides
from ..domain.merchants import create_merchants
from ..generation import batch as batch_module
from ..generation.batch import make_batch
from ..generation.streaming import stream_events


class ProducerBusinessDoDTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.now = datetime(2026, 11, 6, 12, tzinfo=timezone.utc)
        cls.merchants = create_merchants(seed=42)

        cls.batch_duration = 3600 * 4
        cls.batch_arrivals = {}
        def observe_event(index, rng, merchants, arrival_time, late=False):
            event = make_event(
                index,
                rng,
                merchants,
                arrival_time,
                late=late,
            )
            cls.batch_arrivals[event["tx_id"]] = arrival_time
            return event

        with patch.object(
                batch_module,
                make_event.__name__,
                side_effect=observe_event,
        ):
            cls.batch = make_batch(
                50_000,
                cls.merchants,
                42,
                cls.now,
                duration_seconds=cls.batch_duration,
            )

        cls.stream = list(islice(
            stream_events(cls.merchants, 42, clock=lambda: cls.now),
            50_000,
        ))
        cls.datasets = {'lot File': cls.batch, 'flux': cls.stream}

        # Repérer les comptes des scénarios sans imposer leurs identifiants.
        # Cela évite de confondre une convergence aléatoire de transactions
        # ordinaires avec un scénario volontairement injecté.
        rng = random.Random(42)
        overrides = [
            fraud_overrides(index, rng, cls.merchants[0])
            for index in range(5_000)
        ]
        cls.scenario_wallets = {
            item['payer_wallet'] for item in overrides if 'payer_wallet' in item
        }

    def scenario_events(self, events):
        return [
            event for event in events
            if event['payer_wallet'] in self.scenario_wallets
        ]

    def test_file_contient_50000_evenements_jsonl(self):
        """DoD 8 : un fichier de 50 000 lignes JSON, un événement par ligne."""
        with TemporaryDirectory() as directory:
            output = Path(directory) / 'transactions_history.jsonl'
            write_file(output, self.batch)
            lines = output.read_text(encoding='utf-8').splitlines()
            self.assertEqual(len(lines), 50_000)
            for line in lines:
                self.assertIsInstance(json.loads(line), dict)

    def test_schema_des_evenements(self):
        """DoD 10 : les dix champs demandés sont présents dans les deux modes."""
        fields = {
            'tx_id', 'type', 'payer_wallet', 'payee_id', 'device_id',
            'city', 'channel', 'status', 'amount_mad', 'event_time',
        }
        for mode, events in self.datasets.items():
            with self.subTest(mode=mode):
                self.assertTrue(events)
                for event in events:
                    self.assertEqual(set(event), fields)

    def test_presence_p2m_p2p_et_cinq_statuts(self):
        """DoD 11 : toutes les catégories doivent réellement être générées."""
        statuses = {'INITIATED', 'AUTHORIZED', 'COMPLETED', 'REJECTED', 'REVERSED'}
        for mode, events in self.datasets.items():
            with self.subTest(mode=mode):
                self.assertEqual({event['type'] for event in events}, {'P2M', 'P2P'})
                self.assertEqual({event['status'] for event in events}, statuses)

    def test_environ_deux_pourcent_de_doublons(self):
        """DoD 12 : copies de même tx_id ET status, sans égalité complète imposée."""
        for mode, events in self.datasets.items():
            with self.subTest(mode=mode):
                counts = Counter((event['tx_id'], event['status']) for event in events)
                duplicate_count = sum(count - 1 for count in counts.values())
                ratio = duplicate_count / len(events)
                # Convention proposée : 2 % +/- 0,2 point de pourcentage.
                self.assertGreaterEqual(ratio, 0.018)
                self.assertLessEqual(ratio, 0.022)

    def test_environ_cinq_pourcent_de_retards_limites_a_quinze_minutes(self):
        """Retards mesurés par rapport à chaque arrivée simulée."""

        for mode, events in self.datasets.items():
            with self.subTest(mode=mode):
                late_count = 0
                arrivals = []

                for event in events:
                    if mode == "lot File":
                        arrival_time = self.batch_arrivals[event["tx_id"]]
                    else:
                        # Le test du flux utilise toujours une horloge fixe.
                        arrival_time = self.now

                    arrivals.append(arrival_time)

                    event_time = datetime.fromisoformat(
                        event["event_time"].replace("Z", "+00:00")
                    )

                    delay = (arrival_time - event_time).total_seconds()

                    self.assertGreaterEqual(delay, 0)
                    self.assertLessEqual(delay, 15 * 60)
                    late_count += delay > 0

                ratio = late_count / len(events)
                self.assertGreaterEqual(ratio, 0.045)
                self.assertLessEqual(ratio, 0.055)

                if mode == "lot File":
                    self.assertEqual(arrivals[0], self.now)
                    self.assertEqual(
                        arrivals[-1],
                        self.now + timedelta(seconds=self.batch_duration),
                    )
                    self.assertEqual(arrivals, sorted(arrivals))

    def test_scenario_appareil_partage(self):
        """DoD 13 : plusieurs portefeuilles utilisent le même appareil."""
        for mode, events in self.datasets.items():
            with self.subTest(mode=mode):
                wallets_by_device = defaultdict(set)
                for event in self.scenario_events(events):
                    wallets_by_device[event['device_id']].add(event['payer_wallet'])
                self.assertTrue(
                    any(len(wallets) > 1 for wallets in wallets_by_device.values()),
                    'Aucun appareil partagé entre plusieurs comptes des scénarios',
                )

    def test_scenario_transferts_circulaires(self):
        """DoD 13 : un cycle P2P entre plusieurs comptes, sans noms imposés."""
        for mode, events in self.datasets.items():
            with self.subTest(mode=mode):
                edges = {
                    (event['payer_wallet'], event['payee_id'])
                    for event in self.scenario_events(events)
                    if event['type'] == 'P2P'
                       and event['payer_wallet'] != event['payee_id']
                }
                # Éliminer les comptes sans transfert entrant, puis leurs arcs.
                # S'il reste des comptes, le graphe contient un cycle dirigé.
                outgoing = defaultdict(set)
                incoming_count = {}
                for sender, receiver in edges:
                    outgoing[sender].add(receiver)
                    incoming_count.setdefault(sender, 0)
                    incoming_count[receiver] = incoming_count.get(receiver, 0) + 1
                queue = deque(
                    account for account, count in incoming_count.items() if count == 0
                )
                removed = 0
                while queue:
                    account = queue.popleft()
                    removed += 1
                    for receiver in outgoing[account]:
                        incoming_count[receiver] -= 1
                        if incoming_count[receiver] == 0:
                            queue.append(receiver)
                self.assertLess(removed, len(incoming_count), 'Aucun cycle P2P injecté')

    def test_scenario_compte_collecteur(self):
        """DoD 13 : plusieurs émetteurs P2P convergent vers un même compte."""
        for mode, events in self.datasets.items():
            with self.subTest(mode=mode):
                senders_by_receiver = defaultdict(set)
                for event in self.scenario_events(events):
                    if event['type'] == 'P2P' and event['payer_wallet'] != event['payee_id']:
                        senders_by_receiver[event['payee_id']].add(event['payer_wallet'])
                self.assertTrue(
                    any(len(senders) > 1 for senders in senders_by_receiver.values()),
                    'Aucun compte collecteur parmi les scénarios injectés',
                )

    def test_creation_referentiel_absent_avec_300_commercants(self):
        """DoD 14 : création de merchants.json avec 300 commerçants distincts."""
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'merchants.json'
            ensure_merchants(path, seed=42)
            self.assertTrue(path.is_file())
            saved = json.loads(path.read_text(encoding='utf-8'))
            self.assertEqual(len(saved), 300)
            self.assertEqual(len({merchant['merchant_id'] for merchant in saved}), 300)

    def test_utilisation_referentiel_fourni(self):
        """DoD 14 : les commerçants fournis sont réutilisés sans être remplacés."""
        supplied = [dict(merchant) for merchant in self.merchants]
        supplied[0]['name'] = 'Commerçant fourni au test'
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'merchants.json'
            path.write_text(json.dumps(supplied), encoding='utf-8')
            loaded = ensure_merchants(path, seed=999)
            self.assertEqual(loaded, supplied)
            self.assertEqual(json.loads(path.read_text(encoding='utf-8')), supplied)


if __name__ == '__main__':
    unittest.main()
