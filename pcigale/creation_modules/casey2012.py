# -*- coding: utf-8 -*-
# Copyright (C) 2013 Centre de données Astrophysiques de Marseille
# Licensed under the CeCILL-v2 licence - see Licence_CeCILL_V2-en.txt
# Author: Médéric Boquien <mederic.boquien@oamp.fr>

"""
Casey (2012) IR models module
=============================

This module implements the Casey (2012) infra-red models.

"""

import numpy as np
import scipy.constants as cst
from collections import OrderedDict
from . import CreationModule


class Module(CreationModule):
    """Casey (2012) templates IR re-emission

    Given an amount of attenuation (e.g. resulting from the action of a dust
    attenuation module) this module normalises the Casey (2012) template
    corresponding to a given α to this amount of energy and add it to the SED.

    """

    parameter_list = OrderedDict([
        ("temperature", (
            "float",
            "Temperature of the dust in K.",
            None
        )),
        ("beta", (
            "float",
            "Emissivity index of the dust.",
            None
        )),
        ("alpha", (
            "float",
            "Mid-infrared powerlaw slope.",
            None
        )),
        ('attenuation_value_keys', (
            'string',
            "Keys of the SED information dictionary where the module will "
            "look for the attenuation (in W) to re-emit. You can give several "
            "keys separated with a & (don't use commas), a re-emission "
            "contribution will be added for each key.",
            "attenuation"
        ))
    ])

    out_parameter_list = OrderedDict([
        ("temperature", "Temperature of the dust in K."),
        ("beta", "Emissivity index of the dust."),
        ("alpha", "Mid-infrared powerlaw slope.")
    ])


    def _init_code(self):
        """Build the model for a given set of parameters."""

        T = float(self.parameters["temperature"])
        beta = float(self.parameters["beta"])
        alpha = float(self.parameters["alpha"])

        # We define various constants necessary to compute the model. For
        # consistency, we define the speed of light in nm s¯¹ rather than in
        # m s¯¹.
        c = cst.c * 1e9
        b1 = 26.68
        b2 = 6.246
        b3 = 1.905e-4
        b4 = 7.243e-5
        lambda_c = 0.75e3 / ((b1 + b2 * alpha) ** -2. + (b3 + b4 * alpha) * T)
        lambda_0 = 200e3
        Npl = ((1. - np.exp(-(lambda_0 / lambda_c) ** beta)) * (c / lambda_c)
              ** 3. / (np.exp(cst.h * c / (lambda_c * cst.k * T)) - 1.))

        self.wave = np.logspace(3., 6., 1000.)
        conv = c / (self.wave * self.wave)

        self.lumin_blackbody = conv * (1. - np.exp(-(lambda_0 / self.wave)
                              ** beta)) * (c / self.wave) ** 3. / (np.exp(
                              cst.h * c / (self.wave * cst.k * T)) - 1.)
        self.lumin_powerlaw = (conv * Npl * (self.wave / lambda_c) ** alpha *
                        np.exp(-(self.wave / lambda_c) ** 2.))

        # TODO, save the right normalisation factor to retrieve the dust mass
        norm = np.trapz(self.lumin_powerlaw + self.lumin_blackbody,
                        x=self.wave)
        self.lumin_powerlaw /= norm
        self.lumin_blackbody /= norm
        self.lumin = self.lumin_powerlaw + self.lumin_blackbody


    def process(self, sed):
        """Add the IR re-emission contributions.

        Parameters
        ----------
        sed : pcigale.sed.SED object

        """

        # Base name for adding information to the SED.
        name = self.name or 'casey2012'

        attenuation_value_keys = [
            item.strip() for item in
            self.parameters["attenuation_value_keys"].split("&")]

        sed.add_module(name, self.parameters)
        sed.add_info("temperature" + self.postfix, self.parameters["temperature"])
        sed.add_info("alpha" + self.postfix, self.parameters["alpha"])
        sed.add_info("beta" + self.postfix, self.parameters["beta"])

        for attenuation in attenuation_value_keys:
            sed.add_contribution(
                name + '_powerlaw_' + attenuation,
                self.wave,
                sed.info[attenuation] * self.lumin_powerlaw
            )
            sed.add_contribution(
                name + '_blackbody_' + attenuation,
                self.wave,
                sed.info[attenuation] * self.lumin_blackbody
            )
