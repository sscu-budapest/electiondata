from functools import reduce
from hashlib import md5

import datazimmer as dz
import numpy as np
import pandas as pd
from colassigner import get_all_cols

from .. import meta

candidate_table = dz.ScruTable(meta.Candidate)
party_table = dz.ScruTable(meta.Party)
coalition_table = dz.ScruTable(meta.NominatingOrganization)
precinct_table = dz.ScruTable(meta.ElectionPrecinct)

election_table = dz.ScruTable(meta.Election)
geographical_unit_table = dz.ScruTable(meta.GeographicalUnit)
geo_hierarchy_table = dz.ScruTable(meta.DistrictHierarchy)

record_table = dz.ScruTable(meta.VoteRecord)
affiliation_table = dz.ScruTable(meta.Affiliation)
run_table = dz.ScruTable(meta.Run)


def get_eid(df):
    return (
        "hun-"
        + df["held_date"].dt.year.astype(str)
        + "-"
        + np.where(df["is_individual"], "indiv", "party")
        + "-"
        + (df["is_second_round"].astype(int) + 1).astype(str)
    )


def get_geo_id(s, hier_col):
    level = hier_col.split("_")[0]
    return f"hun-{level}" + s.apply(lambda e: md5(e.encode("utf-8")).hexdigest()[:10])


tmp_root = "/home/borza/tmp/election-midway/"
hier_cols = {
    "region_name": meta.DistrictHierarchy.parent.gid,
    "main_name": meta.DistrictHierarchy.child.gid,
}
ext_cols = [
    "külképviseleten szavazók szavazat számlálásra kijelölt",
    "küvi",
    "speciális",
    # "településszintű lakosok",
    "átjelentkezettek",
    "átjelentkezettek szavazására kijelölt",
]

el_cols = [
    "A választópolgárok száma a névjegyzékben a választás befejezésekor",
    "A névjegyzékben és a mozgóurnát igénylő választópolgárok jegyzékében lévő választópolgárok száma",
    "A névjegyzékben és a mozgóurnát igénylő választópolgárok jegyzékében szereplő, a szavazókörben lakcímmel rendelkező választópolgárok száma",
    "A névjegyzékben szereplők száma összesen",
    "A választópolgárok száma összesen",
    "A névjegyzékben lévő, a szavazókörben lakcímmel rendelkező választópolgárok száma",
]

vote_id_cols = ["Jelölő szervezet(ek)", "A pártlista neve, azonosítója", "Lista neve"]
id_cols = ["Jelölt neve", *vote_id_cols]
ind_cols = ["Sorszám", "index", *id_cols]


@dz.register_data_loader
def load_data():
    melt_df = pd.read_parquet(f"{tmp_root}/hun-melt.parquet").rename(
        columns={"inst_url": "meta_id"}
    )
    meta_df = (
        pd.read_parquet(f"{tmp_root}/hun-election-meta.parquet")
        .assign(**{meta.Election.eid: get_eid})
        .rename(columns={"held_date": meta.Election.start_date})
    )

    _put_hierarchy(meta_df)
    _put_locs(meta_df)

    election_table.replace_all(
        meta_df.loc[:, get_all_cols(meta.Election)].drop_duplicates()
    )

    eligible_voter_counts = process_melt(melt_df)
    _put_precinct(meta_df, eligible_voter_counts)


def _put_hierarchy(full_meta: pd.DataFrame):

    geo_hierarchy_table.replace_all(
        full_meta[[*hier_cols.keys()]]
        .drop_duplicates()
        .pipe(lambda df: pd.DataFrame({c: get_geo_id(df[c], c) for c in hier_cols}))
        .rename(columns=hier_cols)
    )


def _put_locs(full_meta: pd.DataFrame):
    geographical_unit_table.purge()
    for hc, (level, bp_level) in zip(
        hier_cols.keys(), [("county", "settlement"), ("settlement", "district")]
    ):
        names_arr = full_meta[hc].drop_duplicates()
        _arr_dic = {
            meta.GeographicalUnit.name: names_arr,
            meta.GeographicalUnit.level_info: np.where(
                names_arr.str.contains("BUDAPEST"), bp_level, level
            ),
            meta.GeographicalUnit.gid: get_geo_id(names_arr, hc),
        }
        geographical_unit_table.extend(pd.DataFrame(_arr_dic))


def get_external_bool(full_meta):

    return (
        full_meta["info"]
        .str.replace("\xa0", "")
        .str.replace("  ", " ")
        .str.replace(" - ", " + ")
        .str.lower()
        .str.split("+", expand=True)
        .pipe(
            lambda df: pd.concat(
                [
                    df.loc[:, c].dropna().str.strip().rename("kw").reset_index()
                    for c in df.columns
                ]
            )
        )
        .assign(c=1)
        .pivot_table(index="meta_id", columns="kw", values="c")
        .fillna(0)
        .astype(bool)
        .loc[:, ext_cols]
        .any(axis=1)
    )


def process_melt(melt_df: pd.DataFrame):

    inded = reduce(
        add_var_ind, ind_cols, remelt(melt_df).rename(columns={"index": "tab_index"})
    ).assign(
        value=lambda df: df["value"].str.replace("\xa0", "").astype(int),
        meta_related=lambda df: df[id_cols].isna().all(axis=1),
    )

    meta_ext = (
        inded.loc[lambda df: df["meta_related"]]
        .assign(rel=lambda df: df["index"].fillna("info"))
        .dropna(how="any", axis=1)
        .pipe(cleanup_varnames)
    )

    vote_base = inded.loc[lambda df: ~df["meta_related"]]
    assert vote_base.loc[:, id_cols[1:]].dropna(how="any").empty

    vote_records = get_vote_records(vote_base)
    record_table.replace_all(vote_records)
    put_runners(vote_records)

    return (
        meta_ext.loc[lambda df: df["rel"].isin(["info", "Pártlista"]), :]
        .pivot_table(index="meta_id", columns="variable", values="value")
        .loc[:, el_cols]
        .max(axis=1)
    )


def split_single_let_based_df(single_let_df):
    return single_let_df.pipe(
        lambda df: df.groupby(df["Szavazóként megjelentek száma összesen"].isna())
    ).pipe(lambda gbo: [gdf.loc[:, gdf.notna().all()] for _, gdf in gbo])


def get_vote_records(vote_pivot: pd.DataFrame):
    _cols = meta.VoteRecord
    renamer = {"meta_id": _cols.precinct.pid, "value": _cols.vote_count}
    return vote_pivot.assign(
        **{
            _cols.org.nid: get_clean_org_name,
            _cols.vid: list(map("vi-{:010d}".format, range(vote_pivot.shape[0]))),
        }
    ).rename(columns=renamer)


def _put_precinct(full_meta, eligible_voter_counts):
    prec_cols = meta.ElectionPrecinct
    hc = "main_name"
    precinct_table.replace_all(
        full_meta.rename(
            columns={
                meta.Election.eid: prec_cols.election.eid,
                "loc": prec_cols.geo_info,
            }
        )
        .assign(
            **{
                prec_cols.geo_parent.gid: lambda df: get_geo_id(df[hc], hc),
                prec_cols.external_votes: get_external_bool(full_meta),
                prec_cols.eligible_voters: eligible_voter_counts,
                prec_cols.name: lambda df: df["main_name"] + "-" + df["loc_id"],
            }
        )
        .fillna(0)
        .reset_index()
        .rename(columns={"meta_id": prec_cols.pid})
    )


def put_runners(vote_records):
    _nidcol = meta.Affiliation.org.nid
    aff_df = (
        vote_records.rename(columns={meta.VoteRecord.org.nid: _nidcol})[[_nidcol]]
        .drop_duplicates()
        .set_index(_nidcol)
        .assign(c=lambda df: df.index.tolist())["c"]
        .str.split("-", expand=True)
        .unstack()
        .dropna()
        .rename(meta.Affiliation.party.pid)
        .reset_index()
    )
    party_table.replace_all(
        aff_df.rename(columns={meta.Affiliation.party.pid: meta.Party.pid})
        .loc[:, [meta.Party.pid]]
        .drop_duplicates()
    )
    coalition_table.replace_all(
        aff_df.rename(columns={_nidcol: meta.NominatingOrganization.nid})
        .loc[:, [meta.NominatingOrganization.nid]]
        .drop_duplicates()
    )
    affiliation_table.replace_all(aff_df)
    run_table.replace_all(
        vote_records.rename(
            columns={
                id_cols[0]: meta.Run.candidate.cid,
                meta.VoteRecord.vid: meta.Run.vote.vid,
            }
        )
        .loc[:, run_table.all_cols]
        .dropna(how="any")
    )


def get_clean_org_name(vote_df):
    return rename_map(
        vote_df.loc[:, vote_id_cols].fillna("").sum(axis=1).str.upper(), org_map
    )


def rename_map(ser, dic):
    return reduce(lambda s, kv: s.str.replace(*kv, regex=False), dic.items(), ser)


def remelt(df):
    return (
        df.assign(variable=lambda df: df["variable"].str.replace("Sorszám", "index"))
        .reset_index()
        .pipe(
            lambda df: df.merge(
                df.loc[
                    (df["variable"].str.len() <= 3) & (df["index"] == 0),
                ]
                .drop(["index"], axis=1)
                .rename(columns={"value": "bup-var"}),
                how="left",
            )
        )
        .loc[lambda df: df["value"] != df["bup-var"], :]
        .assign(
            variable=lambda df: df["bup-var"].where(pd.Series.notna, df["variable"])
        )
        .drop(["bup-var"], axis=1)
    )


def add_var_ind(df, ind_name) -> pd.DataFrame:
    return df.merge(
        df.loc[df["variable"] == ind_name, :]
        .drop(["variable"], axis=1)
        .rename(columns={"value": ind_name}),
        how="left",
    ).loc[lambda _df: _df[ind_name] != _df["value"]]


def cleanup_varnames(df):
    return df.assign(variable=rename_map(df["variable"], var_map))


var_map = {
    "  ": " ",
    "Sorszám": "index",
    "szavazó lapok": "szavazólapok",
    "szavazatok": "szavazólapok",
    "választó polgár": "választópolgár",
    "szavazó választópolgárok": "szavazók",
    "/hiány:": "/ hiányzó:",
    "Eltérés a megjelentek számától (többlet:+ / hiányzó:-)": "Eltérés a szavazóként megjelentek számától (többlet: + / hiányzó: -)",
    "szavazó-helyiségben": "szavazóhelyiségben",
    "Kapott érvényes szavazat": "Kapottérvényesszavazat",
    "névjegyzékben szereplő választópolgárok száma": "névjegyzékben szereplők száma",
    "A névjegyzékben lévő választópolgárok száma": "A névjegyzékben szereplők száma összesen",
    # "A választópolgárok száma összesen": "A névjegyzékben szereplők száma összesen",
    # "A névjegyzékben lévő, a szavazókörben lakcímmel rendelkező választópolgárok száma": ...
    # "Szavazóként megjelentek száma összesen": "Szavazóként megjelentek száma",
}

org_map = {
    "  ": " ",
    "MAGYAR DEMOKRATA FÓRUM": "MDF",
    "KERESZTÉNYDEMOKRATA NÉPPÁRT": "KDNP",
    "MAGYAR IGAZSÁG ÉS ÉLET PÁRTJA": "MIÉP",
    "MAGYAR IGAZSÁG ÉS ÉLET PÁRJA": "MIÉP",
    "FÜGGETLEN KISGAZDAPÁRT": "FKGP",
    "FÜGG. KISGAZDA FÖLDM. POLG. P.": "FKGP",
    "FÜGG.KISGAZDA FÖLDM. POLG. P.": "FKGP",
    "FIATAL DEMOKRATÁK SZÖVETSÉGE": "FIDESZ",
    "MO.-I SZOCIÁLDEMOKRATA PÁRT": "MSZDP",
    "SZABAD DEMOKRATÁK SZÖVETSÉGE": "SZDSZ",
    "FIDESZ MDF": "FIDESZ-MDF",
    "MDF-FIDESZ": "FIDESZ-MDF",
    "MAGYAR SZOCIALISTA PÁRT": "MSZP",
    "MAGYAR SZOCIALISTA MUNKÁSPÁRT": "MSZMP",
    "FÜGGETLEN JELÖLT": "FÜGGETLEN",
    "HAZAFIAS VÁL.KOAL.": "HAZAFIAS VÁLASZTÁSI KOALÍCIÓ",
    "AGRÁRSZÖV.": "AGRÁRSZÖVETSÉG",
    "MO.-I SZÖVETKEZETI AGRÁRPÁRT": "MO. SZÖVETKEZETI AGRÁRPÁRT ",
    "SZ-SZ-B MEGYÉÉRT": "SZSZB MEGYÉÉRT",
    "ÚJ-BAL": "ÚJ BAL",
    "TORGYÁN-KISGAZDA": "TORGYÁN KISGAZDA",
    "FÖLDI-ÉLET-PÁRTJA": "FÖLDI ÉLET PÁRTJA",
    "KISGAZDAPÁRT-MIÉP": "FKGP-MIÉP",
    "PÁRBESZÉD": "PM",
    "CIVIL MOZGALOM": "CM",
    "FÜGGETLEN MAGYAR DEM. PÁRT": "FÜGGETL. MAGYAR DEMOKRATA PÁRT",
    "M NY P": "M.NY.P.",
    # "KISGAZDAPÁRT": "FKGP",
    "KOALICIÓ": "KOALÍCIÓ",
    "ÚJ BALOLDAL": "ÚJ BAL",
}
