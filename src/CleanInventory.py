"""
InventoryCleaner - Retail Data Pipeline
---------------------------------------

Ce module permet de nettoyer et valider les données d’inventaire
dans le cadre d’un pipeline ETL retail.

Étapes :
    1. Vérification de la présence des colonnes essentielles
    2. Validation des types des colonnes brutes
    3. Nettoyage :
        - standardisation du SKU
        - correction du stock (clip + int)
        - séparation Category -> BrandCode + CategoryName
        - suppression des lignes incomplètes
    4. Validation post-cleaning :
        - absence de stocks négatifs
        - BrandCode et CategoryName non vides
        - unicité du SKU
        - format SKU cohérent
    5. Sauvegarde dans /data/clean/

Entrée :
    - Inventory_raw.csv (fichier brut)

Sortie :
    - Inventory_clean.csv (fichier nettoyé prêt pour le modèle en étoile)

Ce fichier est destiné à être utilisé dans un pipeline ETL orchestré
(par exemple via main.py ou un orchestrateur type Airflow).
"""
import pandas as pd
import logging
from pathlib import Path

# Configure le logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class InventoryCleaner:
    def __init__(self, input_path, output_path):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)

    def check_required_columns(self, df, required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logging.error(f"Colonnes manquantes dans Inventory_raw : {missing}")
            raise ValueError(f"Colonnes manquantes : {missing}")
        else:
            logging.info("Toutes les colonnes requises sont présentes.")

    def load_data(self):
        logging.info(f"Loading raw inventory: {self.input_path}")
        df = pd.read_csv(self.input_path)
        return df
    
    def validate_types(self, df):
        if not pd.api.types.is_numeric_dtype(df["Stock"]):
            logging.warning("La colonne 'Stock' n'est pas numérique dans le fichier brut.")
        if not pd.api.types.is_string_dtype(df["SKU Code"]):
            logging.warning("La colonne 'SKU Code' devrait être du texte (string).")
        if not pd.api.types.is_string_dtype(df["Design No."]):
            logging.warning("La colonne 'Design No.' devrait être du texte (string).")
        if not pd.api.types.is_string_dtype(df["Category"]):
            logging.warning("La colonne 'Category' devrait être du texte (string).")
        if (df["SKU Code"].str.len() < 3).any():
            logging.warning("Certains SKU Code ont une longueur anormale (<3).")
        if df["Category"].str.contains(":", regex=False).sum() != len(df):
            logging.warning("Certaines valeurs de 'Category' ne contiennent pas ':'.")



    def clean_data(self, df):
        logging.info("Cleaning inventory data...")

        # 1. Drop lignes vides
        df = df.dropna(how="all")

        # 2. Colonnes essentielles
        df = df.dropna(subset=["SKU Code", "Design No.", "Category"])

        # 3. Standardisation du SKU
        df["SKU Code"] = df["SKU Code"].str.strip().str.upper()

        # 4. Nettoyage du stock
        df["Stock"] = df["Stock"].clip(lower=0).fillna(0).astype(int)

        # 5. Nettoyage de la categorie
        split_col = df["Category"].str.split(":", expand=True)
        df["BrandCode"] = split_col[0].str.strip()
        df["CategoryName"] = split_col[1].str.strip()

        df = df.drop(columns=["Category"])

        # 6. Doublons SKU
        duplicates = df[df.duplicated(subset="SKU Code", keep=False)]
        if not duplicates.empty:
            logging.warning("Duplicate SKU detected:")
            logging.warning(duplicates)

        return df

    def verification_data(self, df):
        issues_found = False  # Flag pour détecter des erreurs
        # Stock négatif
        if (df["Stock"] < 0).any():
            logging.error("Stock négatif détecté après nettoyage !")
            issues_found = True
        # BrandCode vide
        if df["BrandCode"].isna().any() or (df["BrandCode"].str.strip() == "").any():
            logging.error("Certaines lignes ont un BrandCode vide après nettoyage.")
            issues_found = True
        # CategoryName vide
        if df["CategoryName"].isna().any() or (df["CategoryName"].str.strip() == "").any():
            logging.error("Certaines lignes ont un CategoryName vide après nettoyage.")
            issues_found = True
        # SKU dupliqués
        if df["SKU Code"].duplicated().any():
            logging.error("Des SKU en doublon existent après nettoyage, ce qui est interdit dans un inventaire.")
            issues_found = True
        # SKU suspect (pas de tiret)
        if df["SKU Code"].str.contains("-", regex=False).sum() != len(df):
            logging.warning("Certains SKU semblent avoir un format inhabituel (pas de '-').")
        # Message final
        if not issues_found:
            logging.info("Post-cleaning validation passed: données cohérentes et propres.")

    def save_data(self, df):
        logging.info(f"Saving cleaned inventory → {self.output_path}")
        df.to_csv(self.output_path, index=False)

    def run(self):
        df = self.load_data()
        required_cols = ["SKU Code", "Design No.", "Category", "Stock"]
        self.check_required_columns(df, required_cols)
        self.validate_types(df)
        df = self.clean_data(df)
        self.verification_data(df)
        self.save_data(df)

if __name__ == "__main__":
    cleaner = InventoryCleaner(
        input_path="../data/raw/Inventory_raw.csv",
        output_path="../data/clean/Inventory_clean.csv"
    )
    cleaner.run()
