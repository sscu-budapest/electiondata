from hashlib import md5

import datazimmer as dz
import numpy as np
import pandas as pd
from colassigner import get_all_cols

from . import meta

valtor_base = dz.SourceUrl("https://valtor.valasztas.hu")
valtor_list = dz.SourceUrl(f"{valtor_base}/valtort/jsp/tmd1.jsp?TIP=2")

candidate_table = dz.ScruTable(meta.Candidate)
party_table = dz.ScruTable(meta.Party)
coalition_table = dz.ScruTable(meta.NominatingOrganization)
precinct_table = dz.ScruTable(meta.ElectionPrecinct)

election_table = dz.ScruTable(meta.Election)
geographical_unit_table = dz.ScruTable(meta.GeographicalUnit)
geo_hierarchy_table = dz.ScruTable(meta.DistrictHierarchy)

record_table = dz.ScruTable(meta.VoteRecord)
affiliation_table = dz.ScruTable(meta.Affiliation)


def intfy(s):
    return s.fillna("0").str.replace("\xa0", "").astype(int)


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


s3_root = "https://borza-public-data.s3.eu-central-1.amazonaws.com/elections"
hier_cols = {
    "region_name": meta.DistrictHierarchy.parent.gid,
    "main_name": meta.DistrictHierarchy.child.gid,
}
vote_cols = [
    "Sorszám",
    "Kapottérvényesszavazat",
    "Jelölt neve",
    "Jelölő szervezet(ek)",
    "Lista neve",
    "Szavazat",
    "A pártlista neve, azonosítója",
]
ext_cols = [
    "külképviseleten szavazók szavazat számlálásra kijelölt",
    "küvi",
    "speciális",
    # "településszintű lakosok",
    "átjelentkezettek",
    "átjelentkezettek szavazására kijelölt",
]
vote_count_cols = ["Szavazat", "Kapottérvényesszavazat"]
vote_id_cols = ["Jelölő szervezet(ek)", "A pártlista neve, azonosítója", "Lista neve"]


@dz.register_data_loader
def load_data():
    melt_df = pd.read_parquet(f"{s3_root}/hun-melt.parquet")
    meta_df = (
        pd.read_parquet(f"{s3_root}/hun-election-meta.parquet")
        .assign(**{meta.Election.eid: get_eid})
        .rename(columns={"held_date": meta.Election.start_date})
    )

    _put_hierarchy(meta_df)
    _put_locs(meta_df)

    election_table.replace_all(
        meta_df.loc[:, get_all_cols(meta.Election)].drop_duplicates()
    )

    eligible_voter_counts = _process_melt(melt_df)
    _put_precint(meta_df, eligible_voter_counts)


def _put_hierarchy(full_meta):

    geo_hierarchy_table.replace_all(
        full_meta[[*hier_cols.keys()]]
        .drop_duplicates()
        .pipe(lambda df: pd.DataFrame({c: get_geo_id(df[c], c) for c in hier_cols}))
        .rename(columns=hier_cols)
    )


def _put_locs(full_meta):
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


def _get_external_bool(full_meta):

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


def _get_single_letter_based_df(melt_df):
    return (
        melt_df.loc[lambda df: df["variable"].str.len() == 1]
        .groupby(["meta_id", "tab_id", "variable"])["value"]
        .agg(["first", "last"])
        .assign(last=lambda df: df["last"].pipe(intfy))
        .reset_index()
        .pivot_table(index=["meta_id"], columns="first", values="last")
    )


def _process_melt(melt_df):
    single_let_df = _get_single_letter_based_df(melt_df)
    multi_let_df = melt_df.loc[lambda df: df["variable"].str.len() != 1]
    vote_tables_df = multi_let_df.loc[
        lambda df: df["variable"].isin(set(vote_cols))
    ].pipe(
        lambda df: df.loc[:, ["meta_id", "tab_id"]]
        .drop_duplicates()
        .merge(multi_let_df.reset_index())
    )
    multi_let_meta = (
        multi_let_df.reset_index()
        .merge(
            vote_tables_df.loc[:, ["meta_id", "tab_id"]]
            .drop_duplicates()
            .assign(hasvote=True),
            how="left",
        )
        .loc[lambda df: ~df["hasvote"].fillna(False), :]
    )
    vote_pivot = _get_vote_pivot(vote_tables_df)
    vote_records = _get_vote_records(vote_pivot)
    record_table.replace_all(vote_records)
    _put_runners(vote_records)

    multi_pivot = _get_multi_letter_based_pivot(multi_let_meta)
    return _get_eligible_voter_count(single_let_df, multi_pivot)


def _split_single_let_based_df(single_let_df):
    return single_let_df.pipe(
        lambda df: df.groupby(df["Szavazóként megjelentek száma összesen"].isna())
    ).pipe(lambda gbo: [gdf.loc[:, gdf.notna().all()] for _, gdf in gbo])


def _get_vote_pivot(vote_tables_df):

    assert vote_tables_df.groupby("meta_id")["tab_id"].nunique().max() == 1

    vote_pivot = vote_tables_df.pivot_table(
        index=["meta_id", "index"], columns="variable", values="value", aggfunc="first"
    )

    assert all(
        map(
            lambda _cols: vote_pivot.loc[:, _cols].notna().sum(axis=1).unique().tolist()
            == [1],
            [vote_count_cols, vote_id_cols],
        )
    )
    return vote_pivot


def _get_vote_records(vote_pivot):
    _cols = meta.VoteRecord
    renamer = {
        "Jelölt neve": "candidate_name",
        "meta_id": _cols.precinct.pid,
        "index": _cols.vid
    }
    return (
        vote_pivot.assign(
            **{
                _cols.vote_count: lambda df: df.loc[:, vote_count_cols]
                .apply(intfy)
                .sum(axis=1),
                _cols.org.nid: _get_clean_org_name,
            }
        )
        .drop("index", axis=1)
        .reset_index()
        .drop("index", axis=1)
        .reset_index()  # not an accident...
        .rename(columns=renamer)
    )


def _get_multi_letter_based_pivot(multi_let_meta):
    return (
        multi_let_meta.assign(is_pl=lambda df: df["value"] == "Pártlista")
        .assign(
            is_pl_tab=lambda df: df.groupby(["meta_id", "tab_id"])["is_pl"].transform(
                "any"
            ),
            has_pl_tab=lambda df: df.groupby("meta_id")["is_pl"].transform("any"),
        )
        .loc[lambda df: ~df["has_pl_tab"] | (df["has_pl_tab"] & df["is_pl_tab"]), :]
        .loc[
            lambda df: df["value"].str.contains(r"\d", regex=True)
            & ~df["variable"].str.contains("Eltér"),
            :,
        ]
        .assign(
            val=lambda df: df["value"].pipe(intfy),
            variable=lambda df: df["variable"]
            .str.replace("szavazó lapok", "szavazólapok")
            .str.replace("szavazólapok", "szavazatok")
            # .str.replace("választó polgárok", "választópolgárok")
            .str.replace("szavazó-helyiségben", "szavazóhelyiségben").str.replace(
                "szavazó választópolgárok", "szavazók"
            ),
        )
        .pivot_table(index="meta_id", columns="variable", values="val", aggfunc="first")
    )


def _get_eligible_voter_count(single_letter_based_df, multi_pivot):
    vc_col = meta.ElectionPrecinct.eligible_voters
    _df1, _df2 = _split_single_let_based_df(single_letter_based_df)
    df_col_p = [
        (_df1, "A névjegyzékben szereplő választópolgárok száma összesen"),
        (
            _df2,
            "A választópolgárok száma a névjegyzékben a választás befejezésekor",
        ),
    ]

    return pd.concat(
        [
            _df.rename(columns={rcol: vc_col}).loc[:, vc_col].dropna()
            for _df, rcol in df_col_p
        ]
    ).append(
        multi_pivot.loc[
            :,
            lambda df: df.columns.str.contains("névjegy")
            | (df.columns == "A választó polgárok száma összesen")
            | (df.columns == "Átjelentkezett választópolgárok száma"),
        ]
        .fillna(0)
        .pipe(lambda df: df.sum(axis=1))
    )


def _put_precint(full_meta, eligible_voter_counts):
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
                prec_cols.external_votes: _get_external_bool(full_meta),
                prec_cols.eligible_voters: eligible_voter_counts,
                prec_cols.name: lambda df: df["main_name"] + "-" + df["loc_id"],
            }
        )
        .fillna(0)
        .reset_index()
        .rename(columns={"meta_id": prec_cols.pid})
    )


def _put_runners(vote_records):
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


def _get_clean_org_name(vote_df):
    return (
        vote_df.loc[:, vote_id_cols]
        .fillna("")
        .sum(axis=1)
        .str.upper()
        .str.replace("MAGYAR DEMOKRATA FÓRUM", "MDF")
        .str.replace("KERESZTÉNYDEMOKRATA NÉPPÁRT", "KDNP")
        .str.replace("MAGYAR IGAZSÁG ÉS ÉLET PÁRTJA", "MIÉP")
        .str.replace("MAGYAR IGAZSÁG ÉS ÉLET PÁRJA", "MIÉP")
        .str.replace("FÜGGETLEN KISGAZDAPÁRT", "FKGP")
        .str.replace("FÜGG. KISGAZDA FÖLDM. POLG. P.", "FKGP")
        .str.replace("FÜGG.KISGAZDA FÖLDM. POLG. P.", "FKGP")
        .str.replace("FIATAL DEMOKRATÁK SZÖVETSÉGE", "FIDESZ")
        .str.replace("MO.-I SZOCIÁLDEMOKRATA PÁRT", "MSZDP")
        .str.replace("SZABAD DEMOKRATÁK SZÖVETSÉGE", "SZDSZ")
        .str.replace("FIDESZ MDF", "FIDESZ-MDF")
        .str.replace("MDF-FIDESZ", "FIDESZ-MDF")
        .str.replace("MAGYAR SZOCIALISTA PÁRT", "MSZP")
        .str.replace("MAGYAR SZOCIALISTA MUNKÁSPÁRT", "MUNKÁSPÁRT")
        .str.replace("FÜGGETLEN JELÖLT", "FÜGGETLEN")
        .str.replace("HAZAFIAS VÁL.KOAL.", "HAZAFIAS VÁLASZTÁSI KOALÍCIÓ", regex=False)
        .str.replace("AGRÁRSZÖV.", "AGRÁRSZÖVETSÉG", regex=False)
        .str.replace("MO.-I SZÖVETKEZETI AGRÁRPÁRT", "MO. SZÖVETKEZETI AGRÁRPÁRT ")
        .str.replace("SZ-SZ-B MEGYÉÉRT", "SZSZB MEGYÉÉRT")
        .str.replace("ÚJ-BAL", "ÚJ BAL")
        .str.replace("TORGYÁN-KISGAZDA", "TORGYÁN KISGAZDA")
        .str.replace("FÖLDI-ÉLET-PÁRTJA", "FÖLDI ÉLET PÁRTJA")
        .str.replace("KISGAZDAPÁRT-MIÉP", "FKGP-MIÉP")
        .str.replace("PÁRBESZÉD", "PM")
        .str.replace("CIVIL MOZGALOM", "CM")
    )
