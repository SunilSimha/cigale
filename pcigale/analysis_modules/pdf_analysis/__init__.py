# -*- coding: utf-8 -*-
# Copyright (C) 2014 Laboratoire d'Astrophysique de Marseille, AMU
# Copyright (C) 2013 Centre de données Astrophysiques de Marseille
# Copyright (C) 2013-2014 Institute of Astronomy
# Copyright (C) 2013-2014 Yannick Roehlly <yannick@iaora.eu>
# Licensed under the CeCILL-v2 licence - see Licence_CeCILL_V2-en.txt
# Author: Yannick Roehlly, Médéric Boquien & Denis Burgarella

"""
Probability Density Function analysis module
============================================

This module builds the probability density functions (PDF) of the SED
parameters to compute their moments.

The models corresponding to all possible combinations of parameters are
computed and their fluxes in the same filters as the observations are
integrated. These fluxes are compared to the observed ones to compute the
χ² value of the fitting. This χ² give a probability that is associated with
the model values for the parameters.

At the end, for each parameter, the probability-weighted mean and standard
deviation are computed and the best fitting model (the one with the least
reduced χ²) is given for each observation.

"""

from collections import OrderedDict
import multiprocessing as mp
import time

import numpy as np

from .. import AnalysisModule
from .workers import sed as worker_sed
from .workers import init_sed as init_worker_sed
from .workers import init_analysis as init_worker_analysis
from .workers import init_bestfit as init_worker_bestfit
from .workers import analysis as worker_analysis
from .workers import bestfit as worker_bestfit
from ...managers.results import ResultsManager
from ...managers.models import ModelsManager
from ...managers.observations import ObservationsManager
from ...managers.parameters import ParametersManager


class PdfAnalysis(AnalysisModule):
    """PDF analysis module"""

    parameter_list = OrderedDict([
        ("variables", (
            "cigale_string_list()",
            "List of the physical properties to estimate. Leave empty to "
            "analyse all the physical properties (not recommended when there "
            "are many models).",
            ["sfh.sfr", "sfh.sfr10Myrs", "sfh.sfr100Myrs"]
        )),
        ("save_best_sed", (
            "boolean()",
            "If true, save the best SED for each observation to a file.",
            False
        )),
        ("save_chi2", (
            "boolean()",
            "If true, for each observation and each analysed property, save "
            "the raw chi2. It occupies ~15 MB/million models/variable.",
            False
        )),
        ("lim_flag", (
            "boolean()",
            "If true, for each object check whether upper limits are present "
            "and analyse them.",
            False
        )),
        ("mock_flag", (
            "boolean()",
            "If true, for each object we create a mock object "
            "and analyse them.",
            False
        )),
        ("redshift_decimals", (
            "integer()",
            "When redshifts are not given explicitly in the redshifting "
            "module, number of decimals to round the observed redshifts to "
            "compute the grid of models. To disable rounding give a negative "
            "value. Do not round if you use narrow-band filters.",
            2
        )),
        ("blocks", (
            "integer(min=1)",
            "Number of blocks to compute the models and analyse the "
            "observations. If there is enough memory, we strongly recommend "
            "this to be set to 1.",
            1
        ))
    ])


    def _compute_models(self, conf, obs, params, iblock):
        models = ModelsManager(conf, obs, params, iblock)

        initargs = (models, time.time(), mp.Value('i', 0))
        self._parallel_job(worker_sed, params.blocks[iblock], initargs,
                           init_worker_sed, conf['cores'])

        return models

    def _compute_bayes(self, conf, obs, models):
        results = ResultsManager(models)

        initargs = (models, results, time.time(), mp.Value('i', 0))
        self._parallel_job(worker_analysis, obs, initargs,
                           init_worker_analysis, conf['cores'])

        return results

    def _compute_best(self, conf, obs, params, results):
        initargs = (conf, params, obs, results, time.time(),
                    mp.Value('i', 0))
        self._parallel_job(worker_bestfit, obs, initargs,
                           init_worker_bestfit, conf['cores'])

    def _parallel_job(self, worker, items, initargs, initializer, ncores):
        if ncores == 1:  # Do not create a new process
            initializer(*initargs)
            for idx, item in enumerate(items):
                worker(idx, item)
        else:  # run in parallel
            with mp.Pool(processes=ncores, initializer=initializer,
                         initargs=initargs) as pool:
                pool.starmap(worker, enumerate(items))

    def _compute(self, conf, obs, params):
        results = []
        nblocks = len(params.blocks)
        for iblock in range(nblocks):
            print('\nProcessing block {}/{}...'.format(iblock + 1, nblocks))
            # We keep the models if there is only one block. This allows to
            # avoid recomputing the models when we do a mock analysis
            if not hasattr(self, '_models'):
                print("\nComputing models ...")
                models = self._compute_models(conf, obs, params, iblock)
                if nblocks == 1:
                    self._models = models
            else:
                print("\nLoading precomputed models")
                models = self._models

            print("\nEstimating the physical properties ...")
            result = self._compute_bayes(conf, obs, models)
            results.append(result)
            print("\nBlock processed.")

        print("\nEstimating physical properties on all blocks")
        results = ResultsManager.merge(results)

        print("\nComputing the best fit spectra")
        self._compute_best(conf, obs, params, results)

        return results

    def process(self, conf):
        """Process with the psum analysis.

        The analysis is done in two steps which can both run on multiple
        processors to run faster. The first step is to compute all the fluxes
        associated with each model as well as ancillary data such as the SED
        information. The second step is to carry out the analysis of each
        object, considering all models at once.

        Parameters
        ----------
        conf: dictionary
            Contents of pcigale.ini in the form of a dictionary

        """
        np.seterr(invalid='ignore')

        print("Initialising the analysis module... ")

        # Rename the output directory if it exists
        self.prepare_dirs()

        # Store the observations in a manager which sanitises the data, checks
        # all the required fluxes are present, adding errors if needed,
        # discarding invalid fluxes, etc.
        obs = ObservationsManager(conf)
        obs.save('observations')

        # Store the grid of parameters in a manager to facilitate the
        # computation of the models
        params = ParametersManager(conf)

        results = self._compute(conf, obs, params)
        results.best.analyse_chi2()

        print("\nSaving the analysis results...")
        results.save("results")

        if conf['analysis_params']['mock_flag'] is True:
            print("\nAnalysing the mock observations...")

            # For the mock analysis we do not save the ancillary files.
            for k in ['best_sed', 'chi2', 'pdf']:
                conf['analysis_params']["save_{}".format(k)] = False

            # We replace the observations with a mock catalogue..
            obs.generate_mock(results)
            obs.save('mock_observations')

            results = self._compute(conf, obs, params)

            print("\nSaving the mock analysis results...")
            results.save("mock_results")

        print("Run completed!")


# AnalysisModule to be returned by get_module
Module = PdfAnalysis
