"""
Maraston (2005) stellar emission module
=======================================

This module implements the Maraston (2005) Single Stellar Populations.

"""

import numpy as np

from . import SedModule
from ..data import Database


class M2005(SedModule):
    """Maraston (2005) stellar emission module

    This SED creation module convolves the SED star formation history with
    a Maraston (2005) single stellar population to add a stellar component to
    the SED.

    """

    parameter_list = {
        'imf': (
            'cigale_list(dtype=int, options=0. & 1.)',
            "Initial mass function: 0 (Salpeter) or 1 (Kroupa)",
            0
        ),
        'metallicity': (
            'cigale_list(options=0.001 & 0.01 & 0.02 & 0.04)',
            "Metallicity. Possible values are: 0.001, 0.01, 0.02, 0.04.",
            0.02
        ),
        'separation_age': (
            'cigale_list(dtype=int, minvalue=0.)',
            "Age [Myr] of the separation between the young and the old star "
            "populations. The default value in 10^7 years (10 Myr). Set to "
            "0 not to differentiate ages (only an old population).",
            10
        )
    }

    def convolve(self, sfh):
        """Convolve the SSP with a Star Formation History

        Convolves the SSP and the associated info with the SFH.

        Parameters
        ----------
        sfh: array of floats
            Star Formation History in Msun/yr.

        Returns
        -------
        spec_young: array of floats
            Spectrum in W/nm of the young stellar populations.
        spec_old: array of floats
            Same as spec_young but for the old stellar populations.
        info_young: array of floats
            Contains the info for the young stellar populations:
            * 0: total stellar mass
            * 1: alive stellar mass
            * 2: white dwarf stars mass
            * 3: neutron stars mass
            * 4: black holes mass
        info_old: array of floats
            Same as info_young but for the old stellar populations.
        info_all: array of floats
            Same as info_young but for the entire stellar population. Also
            contains 5: stellar mass-weighted age

        """
        # We cut the SSP to the maximum age considered to simplify the
        # computation.
        info_table = self.ssp.info_table[:5, :sfh.size]
        spec_table = self.ssp.spec_table[:, :sfh.size]

        # As both the SFH and the SSP (limited to the age of the SFH) data now
        # share the same time grid, the convolution is just a matter of
        # reverting one and computing the sum of the one to one product; this
        # is done using the dot product. The 1e6 factor is because the SFH is
        # in solar mass per year.
        info_young = 1e6 * np.dot(info_table[:, :self.separation_age],
                                  sfh[-self.separation_age:][::-1])
        spec_young = 1e6 * np.dot(spec_table[:, :self.separation_age],
                                  sfh[-self.separation_age:][::-1])

        info_old = 1e6 * np.dot(info_table[:, self.separation_age:],
                                sfh[:-self.separation_age][::-1])
        spec_old = 1e6 * np.dot(spec_table[:, self.separation_age:],
                                sfh[:-self.separation_age][::-1])

        info_all = info_young + info_old
        info_all = np.append(info_all, np.average(self.ssp.time_grid[:sfh.size],
                                                  weights=info_table[1, :] *
                                                  sfh[::-1]))

        return spec_young, spec_old, info_young, info_old, info_all

    def _init_code(self):
        """Read the SSP from the database."""
        self.imf = int(self.parameters["imf"])
        self.metallicity = float(self.parameters["metallicity"])
        self.separation_age = int(self.parameters["separation_age"])

        if self.imf == 0:
            with Database() as database:
                self.ssp = database.get_m2005('salp', self.metallicity)
        elif self.imf == 1:
            with Database() as database:
                self.ssp = database.get_m2005('krou', self.metallicity)
        else:
            raise Exception(f"IMF #{self.imf} unknown")

    def process(self, sed):
        """Add the convolution of a Maraston 2005 SSP to the SED

        Parameters
        ----------
        sed: pcigale.sed.SED
            SED object.

        """
        out = self.convolve(sed.sfh)
        spec_young, spec_old, info_young, info_old, info_all = out
        lum_young, lum_old = np.trapz([spec_young, spec_old],
                                      self.ssp.wavelength_grid)

        sed.add_module(self.name, self.parameters)

        sed.add_info('stellar.imf', self.imf)
        sed.add_info('stellar.metallicity', self.metallicity)
        sed.add_info('stellar.old_young_separation_age', self.separation_age,
                     unit='Myr')

        sed.add_info('stellar.mass_total_young', info_young[0], True,
                     unit='solMass')
        sed.add_info('stellar.mass_alive_young', info_young[1], True,
                     unit='solMass')
        sed.add_info('stellar.mass_white_dwarf_young', info_young[2], True,
                     unit='solMass')
        sed.add_info('stellar.mass_neutron_young', info_young[3], True,
                     unit='solMass')
        sed.add_info('stellar.mass_black_hole_young', info_young[4], True,
                     unit='solMass')
        sed.add_info('stellar.lum_young', lum_young, True, unit='W')

        sed.add_info('stellar.mass_total_old', info_old[0], True,
                     unit='solMass')
        sed.add_info('stellar.mass_alive_old', info_old[1], True,
                     unit='solMass')
        sed.add_info('stellar.mass_white_dwarf_old', info_old[2], True,
                     unit='solMass')
        sed.add_info('stellar.mass_neutron_old', info_old[3], True,
                     unit='solMass')
        sed.add_info('stellar.mass_black_hole_old', info_old[4], True,
                     unit='solMass')
        sed.add_info('stellar.lum_old', lum_old, True, unit='W')

        sed.add_info('stellar.mass_total', info_all[0], True, unit='solMass')
        sed.add_info('stellar.mass_alive', info_all[1], True, unit='solMass')
        sed.add_info('stellar.mass_white_dwarf', info_all[2], True,
                     unit='solMass')
        sed.add_info('stellar.mass_neutron', info_all[3], True, unit='solMass')
        sed.add_info('stellar.mass_black_hole', info_all[4], True,
                     unit='solMass')
        sed.add_info('stellar.age_mass', info_all[5], unit='Myr')
        sed.add_info('stellar.lum', lum_young + lum_old, True, unit='W')

        sed.add_contribution("stellar.old", self.ssp.wavelength_grid, spec_old)
        sed.add_contribution("stellar.young", self.ssp.wavelength_grid,
                             spec_young)


# SedModule to be returned by get_module
Module = M2005
