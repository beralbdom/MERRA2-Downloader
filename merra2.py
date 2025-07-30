import earthaccess
import netCDF4 as cdf
import pandas as pd
import numpy as np
import os
import requests
import warnings

from rich import print
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from alive_progress import alive_bar

os.system('cls')
os.system('mode con: cols=90 lines=50')

warnings.filterwarnings("ignore", category = UserWarning)       # Alguma coisa estranha com o cdf.num2date
warnings.filterwarnings("ignore", category = RuntimeWarning)    # ^

# https://disc.gsfc.nasa.gov/datasets/M2IMNXASM_5.12.4/summary
print('[bold blue]································ [i]MERRA-2 Downloader v0.1[/i] ·································[/bold blue]')
print('[bold blue]·                  [/bold blue][bold white] Desenvolvido por Bernardo Albuquerque - EPE 2025 [/bold white]                    [bold blue]·[/bold blue]')
print('[bold blue]·              [/bold blue][bold white] Como usar: https://github.com/beralbdom/MERRA2-Downloader[/bold white]                [bold blue]·[/bold blue]')
print('[bold blue]··························································································[/bold blue]\n')

print('[#cccccc]Checando diretórios...[/#cccccc]', end = ' ')
dirs_base = ['MERRA2/Dados brutos', 'MERRA2/Exportado', 'MERRA2/Listas']
if all([os.path.exists(dirs) for dirs in dirs_base]) == False:
    for dir in dirs_base: os.makedirs(dir, exist_ok = True)
    print('\n[red]Algun(s) diretórios ainda não haviam sido criados. Re-execute o script.[/red]')
    os._exit(0)
else: 
    for dir in dirs_base: os.makedirs(dir, exist_ok = True)
print('[#cccccc]OK[/#cccccc]')

print('[#cccccc]Conectando ao EarthAccess...[/#cccccc]', end = ' ')
earthaccess.login(strategy = 'interactive', persist = True)
print('[#cccccc]OK[/#cccccc]')

def download(req):
    url, dir_destino = req
    arq = os.path.basename(urlparse(url).path)
    try:
        caminho_completo = os.path.join(dir_destino, arq)

        if not arq.endswith('.nc4'):
            return

        if os.path.exists(caminho_completo):
            return

        response = requests.get(url, stream = True)
        response.raise_for_status()

        with open(caminho_completo, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)

    except Exception as e:
        print(f'[red]Erro ao processar {arq}: {e}[/red]')
    finally:
        bar()

try:
    print('[#cccccc]Lendo URLs dos arquivos...[/#cccccc]', end = ' ')
    txt_files = [f for f in os.listdir('MERRA2/Listas/') if f.endswith('.txt')]
    print('[#cccccc]OK[/#cccccc]')
    print('\n[green]Baixando dados...[/green]')

    for txt_file in txt_files:
        downloads = []
        dir_nome = os.path.splitext(txt_file)[0]
        dir_destino = os.path.join('MERRA2/Dados brutos', dir_nome)
        os.makedirs(dir_destino, exist_ok = True)

        with open(os.path.join('MERRA2/Listas', txt_file), 'r') as f:
            for line in f:
                if url := line.strip():
                    downloads.append((url, dir_destino))

        with alive_bar(
            len(downloads),
            title = f'Baixando dados do dataset \'{dir_nome}\':',
            bar = None,
            spinner = 'circles',
            enrich_print = False,
            stats = 'tempo restante: {eta}',
            monitor = '{percent:.2%} ({count}/{total})',
            elapsed = None,
            stats_end = False,
        ) as bar:
            with ThreadPoolExecutor(max_workers = 4) as executor:
                executor.map(download, downloads)

except Exception as e:
    print(f'[red]Erro ao ler arquivos: {e}[/red]')
    print('[red]Cheque os arquivos .txt na pasta MERRA2/Listas.[/red]')
    exit()

# def download(req):
#     url, dir_destino = req
#     try:
#         url_parsed = urlparse(url)
#         arq = os.path.basename(url_parsed.path)
#         caminho_completo = os.path.join(dir_destino, arq)
#         if os.path.exists(caminho_completo): return
#         if not arq.endswith('.nc4'): return
#         else:
#             response = requests.get(url, stream = True)
#             response.raise_for_status()

#         with open(caminho_completo, 'wb') as f:
#             for chunk in response.iter_content(chunk_size = 1024):
#                 f.write(chunk)

#     except Exception as e:
#         print(f'[red]Erro ao processar {arq}: {e}[/red]')

#     finally: bar()

# with alive_bar(
#     len(downloads),
#     title = None,
#     bar = None,
#     spinner = 'circles',
#     enrich_print = False,
#     stats = 'tempo restante: {eta}',
#     monitor = '{percent:.2%} ({count}/{total})',
#     elapsed = None,
#     stats_end = False,
# ) as bar:
#     with ThreadPoolExecutor(max_workers = 4) as executor:
#         executor.map(download, downloads)

def processar_netcdf(dir_completo):
    try:
        dataset = cdf.Dataset(dir_completo, 'r')
        time = dataset.variables['time']
        dates = cdf.num2date(time[:], units = time.units, 
                             calendar = getattr(time, 'calendar', 'standard'),
                             only_use_cftime_datetimes = False)
        
        data = pd.to_datetime(dates).to_period('M')
        u_data = dataset.variables['U50M']
        v_data = dataset.variables['V50M']
        data_lats = dataset.variables[u_data.dimensions[1]][:]
        data_lons = dataset.variables[u_data.dimensions[2]][:]

        vel_vento = np.sqrt(u_data[:] ** 2 + v_data[:] ** 2)
        vento_2d = vel_vento.reshape(vel_vento.shape[0], -1)
        lon_grid, lat_grid = np.meshgrid(data_lons, data_lats)
        cols = [f'({lt:.2f}, {ln:.2f})' for lt, ln in zip(lat_grid.ravel(), lon_grid.ravel())]

        return pd.DataFrame(data=vento_2d, index=data, columns=cols)
    
    except Exception as e:
        print(f'[red]Erro ao processar {dir_completo}: {e}[/red]')
        return None
    finally:
        bar()

print('\n[green]Processando datasets...[/green]')
base_data_dir = 'MERRA2/Dados brutos'
export_dir = 'MERRA2/Exportado'
subpastas = [f.path for f in os.scandir(base_data_dir) if f.is_dir()]

for dir_caminho in subpastas:
    dir_nome = os.path.basename(dir_caminho)
    nc_files = [os.path.join(dir_caminho, dataset) for dataset in os.listdir(dir_caminho) if dataset.endswith('.nc4')]
    dfs = []

    if not nc_files:
        print(f'[yellow]Nenhum arquivo .nc4 encontrado em {dir_nome}. Pulando.[/yellow]')
        continue

    with alive_bar(
        len(nc_files),
        title = f'Processando \'{dir_nome}\':',
        bar = None,
        spinner = 'circles',
        enrich_print = False,
        stats = '- tempo restante: {eta}',
        monitor = '{percent:.2%}, {count}/{total}',
        elapsed = None,
        stats_end = False
    ) as bar:
        with ThreadPoolExecutor(max_workers = None) as executor:
            results = executor.map(processar_netcdf, nc_files)
            dfs = [df for df in results if df is not None]

    if dfs:
        folder_df = pd.concat(dfs)
        folder_df = folder_df.groupby(folder_df.index).mean()
        folder_df.sort_index(inplace=True)
        folder_df.index.name = 'Data'
        output_filename = f'{dir_nome}.csv'
        output_path = os.path.join(export_dir, output_filename)
        folder_df.to_csv(output_path)
        print(f'[#cccccc]Exportado para [i]{output_path.replace('\\', '/')}[/i][/#cccccc]\n')


os.system('pause')
# nc_files = []
# for dirpath, _, filenames in os.walk('MERRA2/Dados brutos'):
#     for filename in filenames:
#         if filename.endswith('.nc4'):
#             nc_files.append(os.path.join(dirpath, filename))

# dfs = []
# print('[green]Processando arquivos NetCDF...[/green]\n')

# with alive_bar(
#     len(nc_files),
#     title = None,
#     bar = None,
#     spinner = 'circles',
#     enrich_print = False,
#     stats = 'tempo restante: {eta}',
#     monitor = '{percent:.2%} ({count}/{total}),',
#     elapsed = None,
#     receipt = 'Concluído',
#     stats_end = False,
#     monitor_end = False,
# ) as bar:
#     with ThreadPoolExecutor(max_workers = None) as executor:
#         results = executor.map(process_netcdf, nc_files)
#         dfs = [df for df in results if df is not None]

# if dfs:
#     final_df = pd.concat(dfs)
#     final_df.sort_index(inplace=True)
#     final_df.index.name = 'Data'
#     final_df.to_csv('MERRA2/Exportado/vento_mensal.csv')