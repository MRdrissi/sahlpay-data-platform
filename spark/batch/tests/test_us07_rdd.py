"""SBD-84 : vérifier les analyses RDD sur un historique JSONL de référence."""

import unittest
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from us07_rdd import decode_transaction, filter_transactions, summarize_by_city, top_five_merchants


REFERENCE = Path(__file__).parent / "fixtures" / "us07_reference.jsonl"


class TestAnalysesRDD(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = SparkSession.builder.appName("SahlPay-US07-SBD84").getOrCreate()
        cls.addClassCleanup(spark.stop)
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        lines = REFERENCE.read_text(encoding="utf-8").splitlines()
        cls.events = sc.parallelize(lines, 3).map(decode_transaction).cache()

    def test_lecture_et_doublon(self):
        """11 événements, 10 identifiants distincts et deux copies de la première transaction."""
        ids = self.events.map(lambda event: event["tx_id"]).countByValue()
        self.assertEqual(sum(ids.values()), 11)
        self.assertEqual(len(ids), 10)
        self.assertEqual(ids["TX-0000000001"], 2)

    def test_filtres(self):
        """Le type, le statut et la ville sélectionnent les identifiants attendus."""
        cases = [
            ({"transaction_type": "P2P"}, [8]),
            ({"status": "REJECTED"}, [9]),
            ({"city": "fes"}, [5, 6, 9]),
            ({"transaction_type": "P2M", "status": "COMPLETED", "city": "Rabat"}, [1, 1, 3, 7]),
        ]
        for criteria, expected in cases:
            with self.subTest(criteria=criteria):
                selected = filter_transactions(self.events, **criteria)
                actual = selected.map(lambda event: event["tx_id"]).collect()
                self.assertCountEqual(actual, [f"TX-{number:010d}" for number in expected])

    def test_montants_par_ville(self):
        """Les effectifs, les totaux et les moyennes correspondent aux calculs à la main."""
        self.assertEqual(
            dict(summarize_by_city(self.events).collect()),
            {
                "Rabat": (6, Decimal("540.21"), Decimal("90.04")),
                "Casablanca": (2, Decimal("120.01"), Decimal("60.01")),
                "Fes": (3, Decimal("280.00"), Decimal("93.33")),
            },
        )

    def test_top_cinq_commercants(self):
        """M-005 devance M-006 à égalité pour la cinquième place."""
        self.assertEqual(
            top_five_merchants(self.events),
            [
                ("M-001", Decimal("80.21")),
                ("M-002", Decimal("70.00")),
                ("M-003", Decimal("60.00")),
                ("M-004", Decimal("50.01")),
                ("M-005", Decimal("40.00")),
            ],
        )

    def test_analyses_apres_filtrage(self):
        """Quatre paiements P2M terminés à Rabat totalisent 140,21 MAD."""
        selected = filter_transactions(self.events, "P2M", "COMPLETED", "Rabat")
        self.assertEqual(
            dict(summarize_by_city(selected).collect()),
            {"Rabat": (4, Decimal("140.21"), Decimal("35.05"))},
        )
        self.assertEqual(
            top_five_merchants(selected),
            [("M-001", Decimal("80.21")), ("M-003", Decimal("60.00"))],
        )

    def test_selection_vide(self):
        """Une ville absente produit deux résultats vides."""
        selected = filter_transactions(self.events, city="Oujda")
        self.assertEqual(summarize_by_city(selected).collect(), [])
        self.assertEqual(top_five_merchants(selected), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
