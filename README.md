### MERRA-2 Downloader
###### Script para baixar e processar datasets do MERRA-2. Atualmente apenas dados de vento são suportados. Outros dados são planejados.

#### Variáveis de entrada suportadas:
- `eastward_wind_at_50_meters`
- `northward_wind_at_50_meters`

#### Variáveis de saída suportadas:
- Velocidade do vento a 50 metros;

#### Requisitos:
* Python >3.12
* Bibliotecas de `req.txt` (instale com `pip install -r req.txt`)
* Conta no NASA Earthdata e acesso ao GES DISC
    - [Registrar-se no Earthdata](https://www.earthdata.nasa.gov/data/earthdata-login#toc-how-do-i-register-with-earthdata-login)
    - [Ober acesso ao GES DISC](https://disc.gsfc.nasa.gov/earthdata-login)

---

#### Como usar:
1. Acessar página do dataset no navegador;
1. Em **'Data Access'**, selecione **'Subset / Get Data'**;
1. Em **'Download Method'**, selecione **'Get File Subsets using OPeNDAP'**;
1. Em **'Method Options'**, selecione **'Use ‘Refine Region’ for geo-spatial subsetting'** após definir o intervalo de tempo e região de interesse;
    * **IMPORTANTE!** Caso essa opção não seja selecionada, o download poderá ser significativamente maior.
1. Selecione as variáveis de interesse;
1. Em **'File format'**, selecione **'netCDF'**
1. Clicar em **'Get Data'**;
1. Após o processamento, selecione **'Download Links List'** e mova o arquivo `.txt` baixado para `MERRA2/Listas`.
    - Opcional: Renomeie o arquivo de texto para facilitar sua identificação.
1. Rode o script pelo `MERRA2.bat` ou `python merra2.py`;
1. Os dados extraídos estarão em `MERRA2/Exportado`.
