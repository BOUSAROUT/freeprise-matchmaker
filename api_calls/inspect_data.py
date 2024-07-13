import json
import pandas as pd

# Corrected JSON string
corrected_json_str = """[
    {
        "locationName": "Lyon, Auvergne-Rhône-Alpes, France",
        "entityUrn": "urn:li:fs_position:(ACoAACOt7AkBea4ObwgTm3o0_iVwd3IQFV2-LPc,2409976072)",
        "geoLocationName": "Lyon, Auvergne-Rhône-Alpes, France",
        "geoUrn": "urn:li:fs_geo:103815258",
        "companyName": "Akkodis",
        "timePeriod": {
            "startDate": {
                "month": 5,
                "year": 2024
            }
        },
        "description": "🛠️🔄📊 Rejoint l'équipe Data/Salesforce pour renforcer les capacités d'intégration de données dans Salesforce, répondant aux besoins de plusieurs clients.\\n\\n● Prise en charge des mises à jour et optimisations nécessaires pour améliorer les performances des processus d'intégration de données dans Salesforce.\\n● Travail en étroite collaboration avec les chefs de projet pour définir les besoins métiers, proposer des solutions pertinentes, et faciliter la communication entre les équipes techniques et les parties prenantes.\\n● Responsabilité du chiffrage des évolutions, du suivi des tâches, et de la rédaction de rapports détaillés pour les parties prenantes, garantissant la transparence et l'efficacité de la gestion de projet.",
        "company": {
            "employeeCountRange": {
                "start": 10001
            },
            "industries": [
                "Technologies et services de l’information"
            ]
        },
        "title": "Data Engineer Talend",
        "region": "urn:li:fs_region:(fr,0)",
        "companyUrn": "urn:li:fs_miniCompany:79383535",
        "companyLogoUrl": "https://media.licdn.com/dms/image/C4E0BAQFolAFb8XYDGA/company-logo_"
    }
]"""

# Sample CSV data
data = {
    'firstName': ['Mohamed'],
    'lastName': ['BOUSAROUT'],
    'experience': [corrected_json_str]
}

# Create DataFrame
df = pd.DataFrame(data)

# Function to parse the experience JSON string
def parse_experience(json_str):
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []

# Parse the experience column
df['parsed_experience'] = df['experience'].apply(parse_experience)

# Display the parsed experience data
print(df['parsed_experience'].head(5))
