# Living area mapper
The application finds the most similar living areas from other municipalities for all living areas of the six largest municipalities in Finland. The project consists of three different Python packages that should be run in the following order: [statfin-data-extractor](./data-extraction/src/statfin-data-extractor/), [living-area-features](./feature-engineering/src/living-area-features/) and [living-area-mappings](./predictive-inferences/src/living-area-mappings/). The packages use GCP Cloud Storage as the storage for the input and output data.

Link to the application: https://lookerstudio.google.com/reporting/3dbccbd5-a076-4ee1-b2c3-674714229193/page/ugXgD

## Source data
- https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/
- https://pxdata.stat.fi/PXWeb/pxweb/fi/Postinumeroalueittainen_avoin_tieto

The data is loaded from Stat.fi with [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.fi)-license.

## How to run
1. Change to the folder data-extraction ```cd data-extraction``````
2. Modify cloud parameters in config.toml file
3. Install dependencies: ```poetry install```
4. Spawn a shell in virtual environment: ```poetry shell```
5. Run the command: ```python src/statfin-data-extractor```
6. Repeat all the steps for folders feature-engineering and predictive-inferences