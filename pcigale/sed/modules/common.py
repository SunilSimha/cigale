# -*- coding: utf-8 -*-
"""
Copyright (C) 2012, 2013 Centre de données Astrophysiques de Marseille
Licensed under the CeCILL-v2 licence - see Licence_CeCILL_V2-en.txt

@author: Yannick Roehlly <yannick.roehlly@oamp.fr>

"""


def complete_parameters(given_parameters, parameter_list):
    """Complete the given parameter list with the default values

    Complete the given_parameters dictionary with missing parameters that have
    a default value in the parameter_list. If a parameter from parameter_list
    have no default value and is not present in given_parameters, raises an
    error. If a parameter is present in given_parameters and not in
    parameter_list, an exception is also raised.

    Parameters
    ----------
    given_parameters : dictionary
        Parameter dictionary used to configure the module.
    parameter_list : dictionary
        Parameter list from the module.

    Returns
    -------
    parameters : dictionary
        Dictionary combining the given parameters with the default values for
        the missing ones.

    Raises
    ------
    KeyError when the given parameters are different from the expected ones.

    """
    # For parameters that are present on the parameter_list with a default
    # value and that are not in the giver_parameters dictionary, we add them
    # with their default value.
    for key in parameter_list:
        if (not key in given_parameters) and (
                parameter_list[key][2] is not None):
            given_parameters[key] = parameter_list[key][2]
    # If the keys of the parameters dictionary are different from the one
    # of the parameter_list dictionary, we raises a KeyError. That means
    # that a parameter is missing (and has no default value) or that an
    # unexpected one was given.
    if not set(given_parameters.keys()) == set(parameter_list.keys()):
        missing_parameters = (set(parameter_list.keys())
                              - set(given_parameters.keys()))
        unexpected_parameters = (set(given_parameters.keys())
                                 - set(parameter_list.keys()))
        message = ""
        if missing_parameters:
            message += ("Missing parameters: " +
                        ", ".join(missing_parameters) +
                        ". ")
        if unexpected_parameters:
            message += ("Unexpected parameters: " +
                        ", ".join(unexpected_parameters) +
                        ".")
        raise KeyError("The parameters passed are different from the "
                       "expected one." + message)

    return given_parameters


class SEDCreationModule(object):
    """Abstract class, the pCigale SED creation modules are based on.
    """

    # parameter_list is a dictionary containing all the parameters used by
    # the module. Each parameter name is associate to a tuple (variable type,
    # description [string], default value). Each module must define its
    # parameter list, unless it does not use any parameter. Using None means
    # that there is no description or default value. If None should be the
    # default value, use the 'None' string instead.
    parameter_list = {}

    # out_parameter_list is a dictionary containing all the SED parameters
    # that are added to the SED info dictionary and for which a statistical
    # analysis may be done. Each parameter name is associated with its
    # description. In the SED info dictionary, the parameter name in prefixed
    # with the name of the module plus an underscore (to allow several
    # modules to add a parameter with the same name, for instance a repeated
    # module.)
    out_parameter_list = {}

    # comments is the text that is used to comment the module section in
    # the configuration file. For instance, it can be used to give special
    # instructions for the configuration.
    comments = ""

    def __init__(self, name=None, blank=False, **kwargs):
        """Instantiate a SED creation module

        A name can be given to the module. This can be useful when a same
        module is used several times with different parameters in the SED
        creation process.

        The module parameters must be passed as keyworded parameters. If a
        parameter is not given but exists in the parameter_list with a default
        value, this value is used. If a parameter is missing or if an
        unexpected parameter is given, an error will be raised.

        Parameters
        ----------
        name : string
            Name of the module.
        blank : boolean
            If true, return a non-parameterised module that will be used only
            to query the module parameter list.

        The module parameters must be given as keyworded parameters.

        Raises
        ------
        KeyError : when not all the needed parameters are given or when an
                   unexpected parameter is given.

        """
        self.name = name

        if not blank:
            # Parameters given in constructor.
            parameters = kwargs

            # Complete the parameter dictionary and "export" it to the module
            self.parameters = complete_parameters(parameters,
                                                  self.parameter_list)

            # Run the initialisation code specific to the module.
            self._init_code()

    def _init_code(self):
        """Initialisation code specific to the module.

        For instance, a module taking data in the database can use this method
        to do so, only one time when the module instantiates.

        """
        pass

    def process(self, sed):
        """Process a SED object with the module

        The SED object is updated during the process, one must take care of
        copying it before, if needed.

        Parameters
        ----------
        sed  : pcigale.sed.SED object

        """
        raise NotImplementedError()


def get_module(name, **kwargs):
    """Get a SED creation module from its name

    Parameters
    ----------
    module_name : string
        The name of the module we want to get the class. This name can be
        prefixed by anything using a dot, then the part before the dot is
        used to determine the module to load (e.g. 'dh2002.1' will return
        the 'dh2002' module).

    Returns
    -------
    a pcigale.sed.modules.Module instance
    """
    # Determine the real module name by removing the dotted prefix.
    module_name = name.split('.')[0]

    try:
        # TODO Find a better way to do dynamic import
        import_string = 'from . import ' + module_name + ' as module'
        exec import_string
        return module.Module(name=name, **kwargs)
    except ImportError:
        print('Module ' + module_name + ' does not exists!')
        raise
