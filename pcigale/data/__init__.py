# -*- coding: utf-8 -*-
# Copyright (C) 2012, 2013 Centre de données Astrophysiques de Marseille
# Licensed under the CeCILL-v2 licence - see Licence_CeCILL_V2-en.txt
# Author: Yannick Roehlly

"""
This is the database where we store some data used by pcigale:
 - the information relative to the filters
 - the single stellar populations as defined in Marason (2005)
 - the infra-red templates from Dale and Helou (2002)

The classes for these various objects are described in pcigale.data
sub-packages. The corresponding underscored classes here are used by the
SqlAlchemy ORM to store the data in a unique SQLite3 database.

"""

from pathlib import Path
import pickle
import traceback

import pkg_resources
from sqlalchemy import create_engine, exc, Column, String, Float, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import class_mapper, sessionmaker
import numpy as np

from .filters import Filter
from .themis import THEMIS

DATABASE_FILE = pkg_resources.resource_filename(__name__, 'data.db')

ENGINE = create_engine('sqlite:///' + DATABASE_FILE, echo=False)
BASE = declarative_base()
SESSION = sessionmaker(bind=ENGINE)


class DatabaseLookupError(Exception):
    """
    A custom exception raised when a search in the database does not find a
    result.
    """


class DatabaseInsertError(Exception):
    """
    A custom exception raised when one tries to insert in the database
    something that is already in it.
    """


class _Filter(BASE):
    """ Storage for filters
    """

    __tablename__ = 'filters'

    name = Column(String, primary_key=True)
    description = Column(String)
    trans_table = Column(PickleType)
    pivot_wavelength = Column(Float)

    def __init__(self, f):
        self.name = f.name
        self.description = f.description
        self.trans_table = f.trans_table
        self.pivot_wavelength = f.pivot_wavelength


class _THEMIS(BASE):
    """Storage for the Jones et al (2017) IR models
    """

    __tablename__ = 'THEMIS_models'
    qhac = Column(Float, primary_key=True)
    umin = Column(Float, primary_key=True)
    umax = Column(Float, primary_key=True)
    alpha = Column(Float, primary_key=True)
    wave = Column(PickleType)
    lumin = Column(PickleType)

    def __init__(self, model):
        self.qhac = model.qhac
        self.umin = model.umin
        self.umax = model.umax
        self.alpha = model.alpha
        self.wave = model.wave
        self.lumin = model.lumin


class Database:
    """Object giving access to pcigale database."""

    def __init__(self, writable=False):
        """
        Create a collection giving access to access the pcigale database.

        Parameters
        ----------
        writable: boolean
            If True the user will be able to write new data in the database
            (but he/she must have a writable access to the sqlite file). By
            default, False.
        """
        self.session = SESSION()
        self.is_writable = writable

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def upgrade_base(self):
        """ Upgrade the table schemas in the database
        """
        if self.is_writable:
            BASE.metadata.create_all(ENGINE)
        else:
            raise Exception('The database is not writable.')

    def close(self):
        """ Close the connection to the database

        TODO: It would be better to wrap the database use inside a context
        manager.
        """
        self.session.close_all()

    def add_themis(self, models):
        """
        Add a list of Jones et al (2017) models to the database.

        Parameters
        ----------
        models: list of pcigale.data.THEMIS objects

        """
        if self.is_writable:
            for model in models:
                self.session.add(_THEMIS(model))
            try:
                self.session.commit()
            except exc.IntegrityError:
                self.session.rollback()
                raise DatabaseInsertError(
                    'Error.')
        else:
            raise Exception('The database is not writable.')

    def get_themis(self, qhac, umin, umax, alpha):
        """
        Get the Jones et al (2017) model corresponding to the given set of
        parameters.

        Parameters
        ----------
        qhac: float
            Mass fraction of hydrocarbon solids i.e., a-C(:H) smaller than
        1.5 nm, also known as HAC
        umin: float
            Minimum radiation field
        umin: float
            Maximum radiation field
        alpha: float
            Powerlaw slope dU/dM∝U¯ᵅ

        Returns
        -------
        model: pcigale.data.THEMIS
            The Jones et al (2017) model.

        Raises
        ------
        DatabaseLookupError: if the requested model is not in the database.

        """
        result = (self.session.query(_THEMIS).
                  filter(_THEMIS.qhac == qhac).
                  filter(_THEMIS.umin == umin).
                  filter(_THEMIS.umax == umax).
                  filter(_THEMIS.alpha == alpha).
                  first())
        if result:
            return THEMIS(result.qhac, result.umin, result.umax, result.alpha,
                          result.wave, result.lumin)
        else:
            raise DatabaseLookupError(
                f"The THEMIS model for qhac <{qhac}>, umin <{umin}>, umax "
                f"<{umax}>, and alpha <{alpha}> is not in the database.")

    def _get_parameters(self, schema):
        """Generic function to get parameters from an arbitrary schema.

        Returns
        -------
        parameters: dictionary
            Dictionary of parameters and their values
        """

        return {k.name: np.sort(
                [v[0] for v in set(self.session.query(schema).values(k))])
                for k in class_mapper(schema).primary_key}

    def add_filter(self, pcigale_filter):
        """
        Add a filter to pcigale database.

        Parameters
        ----------
        pcigale_filter: pcigale.data.Filter
        """
        if self.is_writable:
            self.session.add(_Filter(pcigale_filter))
            try:
                self.session.commit()
            except exc.IntegrityError:
                self.session.rollback()
                raise DatabaseInsertError('The filter is already in the base.')
        else:
            raise Exception('The database is not writable.')

    def get_themis_parameters(self):
        """Get parameters for the THEMIS models.

        Returns
        -------
        paramaters: dictionary
            dictionary of parameters and their values
        """
        return self._get_parameters(_THEMIS)

    def add_filters(self, pcigale_filters):
        """
        Add a list of filters to the pcigale database.

        Parameters
        ----------
        pcigale_filters: list of pcigale.data.Filter objects
        """
        if self.is_writable:
            for pcigale_filter in pcigale_filters:
                self.session.add(_Filter(pcigale_filter))
            try:
                self.session.commit()
            except exc.IntegrityError:
                self.session.rollback()
                raise DatabaseInsertError('The filter is already in the base.')
        else:
            raise Exception('The database is not writable.')

    def get_filter(self, name):
        """
        Get a specific filter from the collection

        Parameters
        ----------
        name: string
            Name of the filter

        Returns
        -------
        filter: pcigale.base.Filter
            The Filter object.

        Raises
        ------
        DatabaseLookupError: if the requested filter is not in the database.

        """
        result = (self.session.query(_Filter).
                  filter(_Filter.name == name).
                  first())
        if result:
            return Filter(result.name, result.description, result.trans_table,
                          result.pivot_wavelength)
        else:
            raise DatabaseLookupError(
                f"The filter <{name}> is not in the database")

    def get_filter_names(self):
        """Get the list of the name of the filters in the database.

        Returns
        -------
        names: list
            list of the filter names
        """
        return [n[0] for n in self.session.query(_Filter.name).all()]

    def parse_filters(self):
        """Generator to parse the filter database."""
        for filt in self.session.query(_Filter):
            yield Filter(filt.name, filt.description, filt.trans_table,
                         filt.pivot_wavelength)

    def parse_m2005(self):
        """Generator to parse the Maraston 2005 SSP database."""
        for ssp in self.session.query(_M2005):
            yield M2005(ssp.imf, ssp.metallicity, ssp.time_grid,
                        ssp.wavelength_grid, ssp.info_table, ssp.spec_table)


class SimpleDatabaseEntry:
    """Entry in SimpleDatabase object."""

    def __init__(self, primarykeys, data):
        """Create a dynamically-constructed object. The primary keys and the
        data are passed through two dictionaries. Each key of each dictionary
        is then transformed into an attribute to which the correspond value is
        assigned.

        Parameters
        ----------
        primarykeys: dict
            Dictionary containing the primary keys (e.g., metallicity, etc.)
        data: dict
            Dictionary containing the data (e.g., wavelength, spectrum, etc.)

        """
        for k, v in {**primarykeys, **data}.items():
            setattr(self, k, v)


class SimpleDatabase:
    """Simple database that can contain any data. It is entirely dynamic and
    does not require the database format to be declared. It is created
    on-the-fly when importing the data. The mechanism is that the primary keys
    and the data are passed through two dictionaries. These dictionaries are
    transformed into a SimpleDatabaseEntry where each key corresponds to an
    attribute. This allows to eliminate much of the boilerplate code that is
    needed for an SqlAlchemy database. Each SimpleDatabaseEntry object is saved
    as a pickle file in a directory of the name of the database. So that it is
    straightforward to retrieve the pickle file corresponding to a given set of
    primary keys, the name contains the values of the primary keys. While this
    is very fast, it requires the use to always make queries using the same data
    types for each key (though different keys can have different types). Overall
    a SimpleDatabase is much easier to handle than an SqlAlchemy database.
    """

    def __init__(self, name, writable=False):
        """Prepare the database. Each database is stored in a directory of the
        same name and each entry is a pickle file. We store a specific pickle
        file named parameters.pickle, which is a dictionary that contains the
        values taken by each parameter as a list.

        Parameters
        ----------
        name: str
            Name of the database
        writable: bool
            Flag whether the database should be open as read-only or in write
            mode
        """
        self.name = name
        self.writable = writable
        self.path = Path(pkg_resources.resource_filename(__name__, name))

        if writable is True and self.path.is_dir() is False:
            # Everything looks fine, so we create the database and save a stub
            # of the parameters dictionary.
            self.path.mkdir()

            self.parameters = {}
            with open(self.path / "parameters.pickle", "wb") as f:
                pickle.dump(self.parameters, f)

        # We load the parameters dictionary. If this fails it is likely that
        # something went wrong and it needs to be rebuilt.
        try:
            with open(self.path / "parameters.pickle", "rb") as f:
                self.parameters = pickle.load(f)
        except:
            raise Exception(f"The database {self.name} appears corrupted. "
                            f"Erase {self.path} and rebuild it.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)

        self.close()

    def close(self):
        """Close the database and save the parameters dictionary if the database
        was writable.
        """
        if self.writable is True:
            # Eliminate duplicated parameter values and we save the dictionary.
            for k, v in self.parameters.items():
                self.parameters[k] = list(set(v))

            with open(self.path / "parameters.pickle", "wb") as f:
                pickle.dump(self.parameters, f)

    def add(self, primarykeys, data):
        """Add an entry to the database. The primary keys and the data are used
        to instantiate a SimpleDatabaseEntry object, which is then saved as a
        pickle file. The name of the file is constructed from the names and
        values of the primary keys.

        Parameters
        ----------
        primarykeys: dict
            Dictionary containing the primary keys (e.g., metallicity, etc.)
        data: dict
            Dictionary containing the data (e.g., wavelength, spectrum, etc.)
        """
        if self.writable is False:
            raise Exception(f"The database {self.name} is read-only.")

        entry = SimpleDatabaseEntry(primarykeys, data)
        basename = "_".join(f"{k}={v}" for k, v in sorted(primarykeys.items()))

        with open(self.path / Path(f"{basename}.pickle"), "wb") as f:
            pickle.dump(entry, f)

        if len(self.parameters) == 0:  # Create the initial lists
            for k, v in primarykeys.items():
                self.parameters[k] = [v]
        else:
            for k, v in primarykeys.items():
                self.parameters[k].append(v)

    def get(self, **primarykeys):
        """Get an entry from the database. This is done by loading a pickle file
        whose name is constructed from the names values of the primary keys. It
        is important that for each key the same type is used for adding and
        getting an entry.

        Parameters
        ----------
        primarykeys: keyword argument
            Primary key names and values

        Returns
        -------
        entry: SimpleDatabaseEntry
            Object containing the primary keys (e.g., metallicity, etc.) and the
            data (e.g., wavelength, spectrum, etc.).
        """
        basename = "_".join(f"{k}={v}" for k, v in sorted(primarykeys.items()))

        try:
            with open(self.path / Path(f"{basename}.pickle"), "rb") as f:
                entry = pickle.load(f)
        except:
            raise Exception(f"Cannot read model {primarykeys}. Either the "
                            "parameters were passed incorrectly or the "
                            "database has not been built correctly.")

        return entry
