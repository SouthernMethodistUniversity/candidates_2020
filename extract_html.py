import tempfile, shutil, glob, os, sys, pyarrow
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client

def get_row(row):
    try:
        f = os.path.join("/scratch/group/kbnk_group/projects/00_amazon_etsy_handmade/runs", row['files'])
        tmp = tempfile.mkdtemp(dir="/dev/shm")
        shutil.unpack_archive(f, tmp, 'xztar')
        htmls = glob.glob("{}/**/*.html".format(tmp), recursive=True)
        content = ""
        for html in htmls:
            with open(html, 'r') as h:
                content += h.read()
        shutil.rmtree(tmp)
        return content
    except:
        return "error"

def get_partition(df):
    result = df.apply(lambda row: get_row(row), axis=1)
    return result

def get_html(n_cores):
    df = pd.read_csv('/scratch/group/kbnk_group/projects/00_amazon_etsy_handmade/runs/get_archives.out', names=['files'])
    ddf = dd.from_pandas(df, npartitions=n_cores)
    ddf['content'] = ddf.map_partitions(lambda df: get_partition(df), meta=(str))
    #ddf = client.persist(ddf)
    #df = ddf.compute()
    ddf.to_parquet('/scratch/group/kbnk_group/projects/00_amazon_etsy_handmade/runs', engine='pyarrow')

if __name__ == '__main__':
    n_cores = int(sys.argv[1])
    mem = int(sys.argv[2])
    mem_per_core = mem / n_cores
    print("Cores:\t{}\nMemory:\t{}\nMemory Per Core:\t{}".format(n_cores, mem, mem_per_core))
    client = Client(n_workers=n_cores) #,
    #                threads_per_worker=1,
    #                memory_limit="{}M".format(mem_per_core),
    #                processes=False)
    #client = Client()
    get_html(n_cores)

