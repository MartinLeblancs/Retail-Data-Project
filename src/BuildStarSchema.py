"""
StarSchemaBuilder - Retail Data Model
-------------------------------------

Ce module construit les tables de dimensions et de faits
pour le modèle en étoile à partir des fichiers nettoyés.

Étape actuelle :
    - Construire DimProduct à partir de Inventory_clean.csv
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class StarSchemaBuilder:
    def __init__(self, inventory_clean_path, model_output_dir):
        self.inventory_clean_path = Path(inventory_clean_path)
        self.model_output_dir = Path(model_output_dir)
        self.model_output_dir.mkdir(exist_ok=True)

    def load_inventory(self):
        logging.info(f"Chargement de Inventory_clean depuis {self.inventory_clean_path}")
        df_inventory = pd.read_csv(self.inventory_clean_path)
        return df_inventory

    def build_dim_product(self, df_inventory):
        logging.info("Construction de DimProduct...")

        # Colonnes utiles pour une dimension produit
        cols = ["SKU Code", "Design No.", "Size", "Color", "BrandCode", "CategoryName"]
        dim_product = df_inventory[cols].copy()

        # Ajout de la clé surrogate
        dim_product.insert(0, "ProductKey", range(1, len(dim_product) + 1))

        # Sauvegarde
        output_path = self.model_output_dir / "DimProduct.csv"
        logging.info(f"Sauvegarde de DimProduct → {output_path}")
        dim_product.to_csv(output_path, index=False)

        return dim_product
    
    def build_fact_sales(self, sales_clean_path, dim_product):
        logging.info("Construction de FactSales...")

        df_sales = pd.read_csv(sales_clean_path)

        # 1. Joindre les ventes avec DimProduct
        df_fact = df_sales.merge(
            dim_product[["ProductKey", "SKU Code"]],
            on="SKU Code",
            how="left"
        )

        # 2. Générer DateKey (format YYYYMMDD)
        df_fact["Date"] = pd.to_datetime(df_fact["Date"])
        df_fact["DateKey"] = df_fact["Date"].dt.strftime("%Y%m%d").astype(int)

        # 3. Sélectionner les colonnes finales
        fact_sales = df_fact[[
            "DateKey",
            "ProductKey",
            "Quantity"
        ]].copy()

        # Sauvegarde
        output_path = self.model_output_dir / "FactSales.csv"
        logging.info(f"Sauvegarde de FactSales → {output_path}")
        fact_sales.to_csv(output_path, index=False)

        return fact_sales



    def run(self):
        df_inventory = self.load_inventory()
        dim_product = self.build_dim_product(df_inventory)
        self.build_fact_sales(
            sales_clean_path="../data/clean/Sales_clean.csv",
            dim_product=dim_product
        )


if __name__ == "__main__":
    builder = StarSchemaBuilder(
        inventory_clean_path="../data/clean/Inventory_clean.csv",
        model_output_dir="../data/model"
    )
    builder.run()
