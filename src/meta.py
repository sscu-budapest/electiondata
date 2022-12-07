import datetime as dt

import datazimmer as dz


class Election(dz.AbstractEntity):
    """two elections are considered different
    if it is possible for one person to cast votes in both
    """

    eid = dz.Index & str

    is_individual = bool
    start_date = dt.datetime


class GeographicalUnit(dz.AbstractEntity):
    gid = dz.Index & str

    name = dz.Nullable(str)
    level_info = dz.Nullable(str)


class NominatingOrganization(dz.AbstractEntity):
    nid = dz.Index & str


class Party(dz.AbstractEntity):
    pid = dz.Index & str


class Candidate(dz.AbstractEntity):
    cid = dz.Index & str


class ElectionPrecinct(dz.AbstractEntity):
    pid = dz.Index & str
    name = str
    geo_info = str
    eligible_voters = dz.Nullable(float)
    external_votes = bool
    geo_parent = GeographicalUnit
    election = Election


class VoteRecord(dz.AbstractEntity):

    vid = dz.Index & str

    vote_count = int
    org = NominatingOrganization
    precinct = ElectionPrecinct


class DistrictHierarchy(dz.AbstractEntity):
    parent = GeographicalUnit
    child = GeographicalUnit


class Affiliation(dz.AbstractEntity):
    party = Party
    org = NominatingOrganization


class Run(dz.AbstractEntity):
    vote = VoteRecord
    candidate = Candidate
