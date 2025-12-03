import argparse
from CleanInventory import InventoryCleaner
from CleanSales import SalesCleaner

def main():
    parser = argparse.ArgumentParser(description="Retail Data Pipeline")
    
    parser.add_argument(
        "--clean-inventory",
        action="store_true",
        help="Nettoie Inventory_raw.csv"
    )

    parser.add_argument(
        "--clean-sales",
        action="store_true",
        help="Nettoie Sales_raw.csv"
    )

    parser.add_argument(
        "--clean-all",
        action="store_true",
        help="Nettoie inventaire puis ventes"
    )

    args = parser.parse_args()

    if args.clean_inventory:
        InventoryCleaner(
            input_path="../data/raw/Inventory_raw.csv",
            output_path="../data/clean/Inventory_clean.csv"
        ).run()

    if args.clean_sales:
        SalesCleaner(
            sales_path="../data/raw/Sales_raw.csv",
            inventory_path="../data/clean/Inventory_clean.csv",
            output_path="../data/clean/Sales_clean.csv"
        ).run()

    if args.clean_all:
        InventoryCleaner(
            input_path="../data/raw/Inventory_raw.csv",
            output_path="../data/clean/Inventory_clean.csv"
        ).run()

        SalesCleaner(
            sales_path="../data/raw/Sales_raw.csv",
            inventory_path="../data/clean/Inventory_clean.csv",
            output_path="../data/clean/Sales_clean.csv"
        ).run()

if __name__ == "__main__":
    main()
