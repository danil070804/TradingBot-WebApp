import unittest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from binance_market import asset_to_binance_ticker, calculate_trade_profit


class AssetTickerMappingTests(unittest.TestCase):
    def test_known_aliases(self):
        self.assertEqual(asset_to_binance_ticker("Bitcoin"), "BTCUSDT")
        self.assertEqual(asset_to_binance_ticker("sol"), "SOLUSDT")
        self.assertEqual(asset_to_binance_ticker("DOGE"), "DOGEUSDT")

    def test_preserves_explicit_usdt(self):
        self.assertEqual(asset_to_binance_ticker("ethusdt"), "ETHUSDT")


class TradeProfitTests(unittest.TestCase):
    def test_long_profit(self):
        profit = calculate_trade_profit(amount=100, leverage=10, direction="up", start_price=100, end_price=101)
        self.assertAlmostEqual(profit, 10.0, places=6)

    def test_short_profit(self):
        profit = calculate_trade_profit(amount=100, leverage=10, direction="down", start_price=100, end_price=99)
        self.assertAlmostEqual(profit, 10.0, places=6)

    def test_liquidation_floor(self):
        profit = calculate_trade_profit(amount=100, leverage=20, direction="up", start_price=100, end_price=90)
        self.assertEqual(profit, -100)


if __name__ == "__main__":
    unittest.main()
