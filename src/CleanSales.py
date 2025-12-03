"""
SalesCleaner - Retail Data Pipeline
-----------------------------------

Ce module nettoie et valide les données de ventes afin de les intégrer
dans un modèle en étoile retail.

Étapes :
    1. Vérification des colonnes essentielles
    2. Validation des types bruts
    3. Nettoyage :
        - dates
        - quantités
        - jointure avec inventory
    4. Validation post-cleaning
    5. Sauvegarde dans /data/clean/
"""

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class SalesCleaner:
    def __init__(self, sales_path, inventory_path, output_path):
        self.sales_path = Path(sales_path)
        self.inventory_path = Path(inventory_path)
        self.output_path = Path(output_path)

    def load_data(self):
        logging.info("Chargement des fichiers Sales_raw et Inventory_clean...")
        df_sales = pd.read_csv(self.sales_path)
        df_inventory = pd.read_csv(self.inventory_path)
        return df_sales, df_inventory

    def check_required_columns(self, df, required_cols):
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logging.error(f"Colonnes manquantes : {missing}")
            raise ValueError(f"Colonnes manquantes : {missing}")
        logging.info("Toutes les colonnes requises sont présentes.")

    def validate_types(self, df):
        if not pd.api.types.is_numeric_dtype(df["Quantity"]):
            logging.warning("La colonne 'Quantity' n'est pas numérique dans le fichier brut.")
        if not pd.api.types.is_string_dtype(df["SKU Code"]):
            logging.warning("La colonne 'SKU Code' devrait être du texte (string).")
        parsed_dates = pd.to_datetime(df["Date"], errors="coerce")
        if parsed_dates.isna().any():
            logging.warning("Certaines valeurs de 'Date' sont invalides ou mal formatées.")
        if (df["SKU Code"].str.len() < 3).any():
            logging.warning("Certains SKU Code ont une longueur anormale (<3).")

    def clean_data(self, df_sales, df_inventory):
        logging.info("Nettoyage des données sales...")

        # 1. Drop lignes vides
        df_sales = df_sales.dropna(how="all")

        # 2. Colonnes essentielles
        df_sales = df_sales.dropna(subset=["SKU Code", "Quantity", "Date"])

        # 3. Nettoyage Date
        df_sales["Date"] = pd.to_datetime(df_sales["Date"], errors="coerce")
        df_sales = df_sales.dropna(subset=["Date"])

        # 4. Nettoyage Quantity
        df_sales["Quantity"] = pd.to_numeric(df_sales["Quantity"], errors="coerce")
        df_sales["Quantity"] = df_sales["Quantity"].fillna(0).astype(int)
        
        # Normalisation du SKU (pour cohérence avec inventory)
        df_sales["SKU Code"] = df_sales["SKU Code"].str.strip().str.upper()

        # Quantités négatives (rare mais possible)
        if (df_sales["Quantity"] < 0).any():
            logging.warning("Certaines quantités sont négatives — elles seront forcées à 0.")
            df_sales["Quantity"] = df_sales["Quantity"].clip(lower=0)

        # 5. Filtrer les SKU validés depuis inventory_clean
        valid_skus = set(df_inventory["SKU Code"].dropna())
        before = len(df_sales)
        df_sales = df_sales[df_sales["SKU Code"].isin(valid_skus)]
        dropped = before - len(df_sales)

        if dropped > 0:
            logging.warning(f"{dropped} lignes supprimées car SKU non trouvés dans Inventory_clean.")

        return df_sales


    def post_validation(self, df):
        issues_found = False
        # 1. Quantités négatives
        if (df["Quantity"] < 0).any():
            logging.error("Des quantités négatives sont présentes après nettoyage.")
            issues_found = True
        # 2. Dates manquantes
        if df["Date"].isna().any():
            logging.error("Des dates sont manquantes après nettoyage.")
            issues_found = True
        # 3. Dates dans le futur (erreur typique)
        future_dates = df[df["Date"] > pd.Timestamp.today()]
        if not future_dates.empty:
            logging.warning(f"{len(future_dates)} ventes ont des dates dans le futur.")
        # 4. SKU vides ou corrompus
        if df["SKU Code"].isna().any() or (df["SKU Code"].str.strip() == "").any():
            logging.error("Certains SKU Code sont vides après nettoyage.")
            issues_found = True
        # 5. Vérification quantités anormalement élevées
        if (df["Quantity"] > 5000).any():
            logging.warning("Certaines quantités semblent anormalement élevées (> 5000).")
        # Résultat final
        if not issues_found:
            logging.info("Post-cleaning validation passed: données cohérentes et exploitables.")


    def save_data(self, df):
        logging.info(f"Sauvegarde du fichier cleaned → {self.output_path}")
        df.to_csv(self.output_path, index=False)

    def run(self):
        df_sales, df_inventory = self.load_data()

        required_cols = ["SKU Code", "Quantity", "Date"]
        self.check_required_columns(df_sales, required_cols)

        self.validate_types(df_sales)

        df_cleaned = self.clean_data(df_sales, df_inventory)

        self.post_validation(df_cleaned)

        self.save_data(df_cleaned)


if __name__ == "__main__":
    cleaner = SalesCleaner(
        sales_path="../data/raw/Sales_raw.csv",
        inventory_path="../data/clean/Inventory_clean.csv",
        output_path="../data/clean/Sales_clean.csv"
    )
    cleaner.run()
