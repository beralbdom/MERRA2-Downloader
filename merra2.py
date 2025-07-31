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
print('[bold blue]································ [i]MERRA-2 Downloader v0.3[/i] ·································[/bold blue]')
print('[bold blue]·                                                                                        ·[/bold blue]')
print('[bold blue]·             [/bold blue][bold white] Como usar: https://github.com/beralbdom/MERRA2-Downloader[/bold white]                 [bold blue]·[/bold blue]')
print('[bold blue]·                                                                                        ·[/bold blue]')
print('[bold blue]···················· [i]Desenvolvido por Bernardo Albuquerque - EPE 2025[/i] ····················[/bold blue]\n')

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
    caminho_completo = os.path.join(dir_destino, arq)

    if not arq.endswith('.nc4'):
        bar()
        return

    if os.path.exists(caminho_completo):
        bar()
        return

    for tentativa in range(3):
        try:
            response = requests.get(url, stream = True)
            response.raise_for_status()

            with open(caminho_completo, 'wb') as f:
                for chunk in response.iter_content(chunk_size = 1024):
                    f.write(chunk)
            break

        except Exception as e:
            print(f'[red]Erro ao baixar {arq}: {e}[/red]')
            os.remove(caminho_completo) if os.path.exists(caminho_completo) else None

            if tentativa < 2:
                print(f'[yellow]Tentando novamente... ({tentativa + 1}/3)[/yellow]')
            else:
                print(f'[red]Falha ao baixar {arq} após 3 tentativas.[/red]')   
    bar()

def processar_netcdf(dir_completo):
    try:
        dataset = cdf.Dataset(dir_completo, 'r')
        time_var = dataset.variables['time']
        datas = cdf.num2date(time_var[:], units = time_var.units, only_use_cftime_datetimes = False)
        datas_dt = pd.to_datetime(datas).to_period('M')
        vars_uteis = [var for var in dataset.variables if var not in ['time', 'lat', 'lon']]
        dfs = {}

        if vars_uteis:
            var_0 = dataset.variables[vars_uteis[0]]
            var_lats = dataset.variables[var_0.dimensions[1]][:]
            var_lons = dataset.variables[var_0.dimensions[2]][:]
            lon_grid, lat_grid = np.meshgrid(var_lons, var_lats)
            cols = [f'({lt:.2f}, {ln:.2f})' for lt, ln in zip(lat_grid.ravel(), lon_grid.ravel())]

        if 'U50M' in vars_uteis and 'V50M' in vars_uteis:
            u_data = dataset.variables['U50M'][:]
            v_data = dataset.variables['V50M'][:]
            vel_vento = np.sqrt(u_data ** 2 + v_data ** 2)
            vento_2d = vel_vento.reshape(vel_vento.shape[0], -1)
            dfs['vento_50m'] = pd.DataFrame(data = vento_2d, index = datas_dt, columns = cols)

        for var_name in vars_uteis:
            var_data = dataset.variables[var_name]
            var_values = var_data[:]
            var_2d = var_values.reshape(var_values.shape[0], -1)
            dfs[var_name] = pd.DataFrame(data = var_2d, index = datas_dt, columns = cols)

        return dfs   
    
    except Exception as e:
        print(f'[red]Erro ao processar {dir_completo}: {e}[/red]')
        return None
    finally:
        bar()


try:
    print('[#cccccc]Lendo URLs dos arquivos...[/#cccccc]', end = ' ')
    arqs_txt = [f for f in os.listdir('MERRA2/Listas/') if f.endswith('.txt')]
    if not arqs_txt:
        print('\n[red]Nenhum arquivo .txt encontrado na pasta MERRA2/Listas.[/red]')
        exit()
    print('[#cccccc]OK[/#cccccc]')
    print('\n[green]Baixando dados...[/green]')

    for txt in arqs_txt:
        downloads = []
        dir_nome = os.path.splitext(txt)[0]
        dir_destino = os.path.join('MERRA2/Dados brutos', dir_nome)
        os.makedirs(dir_destino, exist_ok = True)

        with open(os.path.join('MERRA2/Listas', txt), 'r') as f:
            for line in f:
                if url := line.strip():
                    downloads.append((url, dir_destino))

        with alive_bar(
            len(downloads),
            title = f'Baixando dados do dataset \'{dir_nome}\':',
            bar = None,
            spinner = 'circles',
            enrich_print = False,
            stats = '{eta}',
            monitor = '{percent:.2%}, ({count}/{total})',
            elapsed = None,
            stats_end = False,
        ) as bar:
            with ThreadPoolExecutor(max_workers = 4) as executor:
                executor.map(download, downloads)
      
except Exception as e:
    print(f'[red]Erro ao ler arquivos: {e}[/red]')
    print('[red]Cheque os arquivos .txt na pasta MERRA2/Listas.[/red]')
    exit()
    os.system('pause')


print('\n[green]Processando datasets...[/green]')
dados_dir = 'MERRA2/Dados brutos'
export_dir = 'MERRA2/Exportado'
subpastas = [f.path for f in os.scandir(dados_dir) if f.is_dir()]

for dir_caminho in subpastas:
    dir_nome = os.path.basename(dir_caminho)
    os.makedirs(f'{export_dir}/{dir_nome}', exist_ok = True)

    cdfs = [os.path.join(dir_caminho, dataset) for dataset in os.listdir(dir_caminho) if dataset.endswith('.nc4')]
    dfs = []

    if not cdfs:
        print(f'[yellow]Nenhum arquivo .nc4 encontrado em {dir_nome}. Pulando.[/yellow]')
        continue

    with alive_bar(
        len(cdfs),
        title = f'Processando \'{dir_nome}\':',
        bar = None,
        spinner = 'circles',
        enrich_print = False,
        stats = '{eta}',
        monitor = '{percent:.2%}, {count}/{total}',
        elapsed = None,
        stats_end = False
    ) as bar:
        with ThreadPoolExecutor(max_workers = None) as executor:
            results = executor.map(processar_netcdf, cdfs)

            dfs_por_variavel = {}
            for res in results:
                if res:
                    for var, df in res.items():
                        if var not in dfs_por_variavel:
                            dfs_por_variavel[var] = []
                        dfs_por_variavel[var].append(df)

    if dfs_por_variavel:
        for var, dfs in dfs_por_variavel.items():
            folder_df = pd.concat(dfs)

            folder_df.sort_index(inplace = True)
            folder_df.index.name = 'Data'

            dir_saida = f'{export_dir}/{dir_nome}/{var}.csv'
            folder_df.to_csv(dir_saida)

            print(f'[#cccccc]Salvo em [i]{dir_saida.replace('\\', '/')}[/i][/#cccccc]')
        
        print('')

os.system('pause')