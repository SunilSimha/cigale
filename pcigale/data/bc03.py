# -*- coding: utf-8 -*-
# Copyright (C) 2013 Centre de données Astrophysiques de Marseille
# Licensed under the CeCILL-v2 licence - see Licence_CeCILL_V2-en.txt
# Author: Yannick Roehlly

import numpy as np


class BC03:
    """Single Stellar Population as defined in Bruzual and Charlot (2003)

    This class holds the data associated with a single stellar population
    (SSP) as defined in Bruzual and Charlot (2003). Compared to the pristine
    Bruzual and Charlot SSP:
        * The time grid ranges from 1 Myr to 14 Gyr with 1 Myr steps.
        * The SSP are all interpolated on this new grid.
        * The wavelength grid is refined beyond 10 μm to avoid artefacts.
        * The wavelength is given in nm rather than Å.
        * The spectra are given in W/nm rather than Lsun.

    """

    def __init__(self, imf, metallicity, time_grid, wavelength_grid,
                 info_table, spec_table):
        """Create a new single stellar population as defined in Bruzual and
        Charlot (2003).

        Parameters
        ----------
        imf: string
            Initial mass function (IMF): either 'salp' for Salpeter (1955) or
            'chab' for Chabrier (2003).
        metallicity: float
            The metallicity. Possible values are 0.0001, 0.0004, 0.004, 0.008,
            0.02, and 0.05.
        time_grid: array of floats
            The time grid in Myr used in the info_table and the spec_table.
        wavelength_grid: array of floats
            The wavelength grid in nm used in spec_table.
        info_table: 2 axis array of floats
            Array containing information from some of the *.?color tables from
            Bruzual and Charlot (2003) at each time of the time_grid.
                * info_table[0]: Total mass in stars in solar mass
                * info_table[1]: Mass returned to the ISM by evolved stars in
                    solar mass
                * info_table[2]: rate of H-ionizing photons (s-1)
        spec_table: 2D array of floats
            Spectrum of the SSP in W/nm (first axis) every 1 Myr (second axis).

        """

        if imf in ['salp', 'chab']:
            self.imf = imf
        else:
            raise ValueError('IMF must be either sal for Salpeter or '
                             'cha for Chabrier.')
        self.metallicity = metallicity
        self.time_grid = time_grid
        self.wavelength_grid = wavelength_grid
        self.info_table = info_table
        self.spec_table = spec_table
