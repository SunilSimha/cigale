import argparse
import multiprocessing as mp
import sys

from astropy.table import Table, Column
import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np

from pcigale.data import Database, Filter


def list_filters():
    """Print the list of filters in the pcigale database.
    """
    with Database() as base:
        filters = {name: base.get_filter(name) for name in
                   base.get_filter_names()}

    name = Column(data=[filters[f].name for f in filters], name='Name')
    description = Column(data=[filters[f].description for f in filters],
                         name='Description')
    wl = Column(data=[filters[f].pivot_wavelength for f in filters],
                name='Pivot Wavelength', unit=u.nm, format='%d')
    samples = Column(data=[filters[f].trans_table[0].size for f in filters],
                     name="Points")

    t = Table()
    t.add_columns([name, description, wl, samples])
    t.sort(['Pivot Wavelength'])
    t.pprint(max_lines=-1, max_width=-1)


def add_filters(fnames):
    """Add filters to the pcigale database.
    """
    with Database(writable=True) as base:
        for fname in fnames:
            with open(fname, 'r') as f_fname:
                filter_name = f_fname.readline().strip('# \n\t')
                filter_type = f_fname.readline().strip('# \n\t')
                filter_description = f_fname.readline().strip('# \n\t')
            filter_table = np.genfromtxt(fname)
            # The table is transposed to have table[0] containing the
            # wavelength and table[1] containing the transmission.
            filter_table = filter_table.transpose()

            # We convert the wavelength from Å to nm.
            filter_table[0] *= 0.1

            # We convert to energy if needed
            if filter_type == 'photon':
                filter_table[1] *= filter_table[0]
            elif filter_type != 'energy':
                raise ValueError("Filter transmission type can only be "
                                 "'energy' or 'photon'.")

            print(f"Importing {filter_name}... "
                  f"({filter_table.shape[1]} points)")

            new_filter = Filter(filter_name, filter_description, filter_table)

            # We normalise the filter and compute the pivot wavelength. If the
            # filter is a pseudo-filter used to compute line fluxes, it should
            # not be normalised.
            if not (filter_name.startswith('PSEUDO') or
                    filter_name.startswith('linefilter.')):
                new_filter.normalise()
            else:
                new_filter.pivot_wavelength = np.mean(
                    filter_table[0][filter_table[1] > 0]
                )

            base.add_filter(new_filter)


def worker_plot(fname):
    """Worker to plot filter transmission curves in parallel

    Parameters
    ----------
    fname: string
        Name of the filter to be plotted
    """
    with Database() as base:
        _filter = base.get_filter(fname)

    if _filter.pivot_wavelength >= 1e3 and _filter.pivot_wavelength < 1e6:
        _filter.trans_table[0] *= 1e-3
        unit = "μm"
    elif _filter.pivot_wavelength >= 1e6 and _filter.pivot_wavelength < 1e7:
        _filter.trans_table[0] *= 1e-6
        unit = "mm"
    elif _filter.pivot_wavelength >= 1e7:
        _filter.trans_table[0] *= 1e-7
        unit = "cm"
    else:
        unit = "nm"

    _filter.tr *= 1. / np.max(_filter.tr)

    plt.clf()
    plt.plot(_filter.trans_table[0], _filter.trans_table[1], color='k')
    plt.xlim(_filter.trans_table[0][0], _filter.trans_table[0][-1])
    plt.minorticks_on()
    plt.xlabel(f'Wavelength [{unit}]')
    plt.ylabel('Relative transmission')
    plt.title(f"{fname} filter")
    plt.tight_layout()
    plt.savefig(f"{fname}.pdf")


def plot_filters(fnames):
    """Plot the filters provided as parameters. If not filter is given, then
    plot all the filters.
    """
    if len(fnames) == 0:
        with Database() as base:
            fnames = base.get_filter_names()
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.map(worker_plot, fnames)


def main():

    if sys.version_info[:2] >= (3, 4):
        mp.set_start_method('spawn')
    else:
        print("Could not set the multiprocessing start method to spawn. If "
              "you encounter a deadlock, please upgrade to Python≥3.4.")

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(help="List of commands")

    list_parser = subparsers.add_parser('list', help=list_filters.__doc__)
    list_parser.set_defaults(parser='list')

    add_parser = subparsers.add_parser('add', help=add_filters.__doc__)
    add_parser.add_argument('names', nargs='+', help="List of file names")
    add_parser.set_defaults(parser='add')

    plot_parser = subparsers.add_parser('plot', help=plot_filters.__doc__)
    plot_parser.add_argument('names', nargs='*', help="List of filter names")
    plot_parser.set_defaults(parser='plot')

    if len(sys.argv) == 1:
        parser.print_usage()
    else:
        args = parser.parse_args()
        if args.parser == 'list':
            list_filters()
        elif args.parser == 'add':
            add_filters(args.names)
        elif args.parser == 'plot':
            plot_filters(args.names)
