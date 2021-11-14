from itertools import product
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import multiprocessing as mp
import numpy as np

from pcigale.utils.io import read_table
from pcigale.utils.console import console, INFO
from pcigale.utils.counter import Counter


def pool_initializer(counter):
    """Initializer of the pool of processes to share variables between workers.
    Parameters
    ----------
    :param counter: Counter class object for the number of models plotted
    """
    global gbl_counter

    gbl_counter = counter


def _parallel_job(worker, items, initargs, initializer, ncores, chunksize=None):
    if ncores == 1:  # Do not create a new process
        initializer(*initargs)
        for item in items:
            worker(*item)
    else:  # run in parallel
        # Temporarily remove the counter sub-process that updates the
        # progress bar as it cannot be pickled when creating the parallel
        # processes when using the "spawn" starting method.
        for arg in initargs:
            if isinstance(arg, Counter):
                counter = arg
                progress = counter.progress
                counter.progress = None

        with mp.Pool(
            processes=ncores, initializer=initializer, initargs=initargs
        ) as pool:
            pool.starmap(worker, items, chunksize)

        # After the parallel processes have exited, it can be restored
        counter.progress = progress

def chi2(config, format, outdir):
    """Plot the χ² values of analysed variables.
    """
    configuration = config.configuration
    file = outdir.parent / configuration['data_file']
    input_data = read_table(file)
    save_chi2 = configuration['analysis_params']['save_chi2']

    chi2_vars = []
    if 'all' in save_chi2 or 'properties' in save_chi2:
        chi2_vars += configuration['analysis_params']['variables']
    if 'all' in save_chi2 or 'fluxes' in save_chi2:
        chi2_vars += configuration['analysis_params']['bands']

    items = list(product(input_data['id'], chi2_vars, [format], [outdir]))
    counter = Counter(len(items), 1, "Item")

    _parallel_job(_chi2_worker,
        items,
        (counter, ),
        pool_initializer,
        configuration["cores"]
    )

    # Print the final value as it may not otherwise be printed
    counter.global_counter.value = len(items)
    counter.progress.join()
    console.print(f"{INFO} Done.")


def _chi2_worker(obj_name, var_name, format, outdir):
    """Plot the reduced χ² associated with a given analysed variable

    Parameters
    ----------
    obj_name: string
        Name of the object.
    var_name: string
        Name of the analysed variable..
    outdir: Path
        Path to outdir

    """
    gbl_counter.inc()
    figure = plt.figure()
    ax = figure.add_subplot(111)

    var_name = var_name.replace('/', '_')
    fnames = outdir.glob(f"{obj_name}_{var_name}_chi2-block-*.npy")
    for fname in fnames:
        data = np.memmap(fname, dtype=np.float64)
        data = np.memmap(fname, dtype=np.float64, shape=(2, data.size // 2))
        ax.scatter(data[1, :], data[0, :], color='k', s=.1)
    ax.set_xlabel(var_name)
    ax.set_ylabel(r"Reduced $\chi^2$")
    ax.set_ylim(0., )
    ax.minorticks_on()
    figure.suptitle(f"Reduced $\chi^2$ distribution of {var_name} for "
                    f"{obj_name}.")
    figure.savefig(outdir / f"{obj_name}_{var_name}_chi2.{format}")
    plt.close(figure)
