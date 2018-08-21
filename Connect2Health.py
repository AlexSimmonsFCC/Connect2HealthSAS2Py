import pandas as pd
import pandasql as psql
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import os
from sqlite3 import connect
from pandas import read_sql_query
import pandasql
import numpy
pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', 500)
os.chdir(r"N:\Connect2Health")
workdir = r'N:\Connect2Health'

#TODO Make original variable names more explicit
#TODO Check all table and column names are consistent with SAS
#Read in blockmaster_2014.csv into DataFrame as 'ab_bm2014'
file_blockmaster2014 = "blockmaster2014.csv"
columnNames_ab_bm2014 = ["BlockCode", "state_fips", "stateabbr", "county_fips", "county_name", "urban_rural", "population", "housing_units"]
ab_bm2014_parsed = pd.read_csv(file_blockmaster2014, delimiter="|", names=columnNames_ab_bm2014, skiprows=1, usecols=[0, 1, 2, 4, 5, 10, 11, 12], dtype={'BlockCode':object})
ab_bm2014_parsed_sorted = ab_bm2014_parsed.sort_values('BlockCode')
ab_bm2014_parsed_sorted_sampled = ab_bm2014_parsed_sorted.head(10000)

#Read in geolytics.csv into DataFrame as 'file_geolytics2014'
file_geolytics2014 = "geolytics_2014.csv"
geolytics2014_parsed = pd.read_csv(file_geolytics2014,  delimiter="|", names=["BlockCode", "gl_pop", "gl_housingunits", "gl_households"], skiprows=1, dtype={'BlockCode':object})

#Inner Join geolytics2014_parsed and ab_bm2014 by BlockCode
geolytics2014_parsed_sorted = geolytics2014_parsed.sort_values('BlockCode', ascending=True)
geolytics2014_parsed_sorted_sampled = geolytics2014_parsed_sorted.head(10000)
ab_popfile_urbanrural = geolytics2014_parsed_sorted_sampled.set_index('BlockCode').join(ab_bm2014_parsed_sorted_sampled.set_index('BlockCode'))
ab_popfile_urbanrural_joined = geolytics2014_parsed_sorted_sampled.merge(ab_bm2014_parsed_sorted_sampled, how="inner", on="BlockCode")
popfile_rural_blockgroups = ab_popfile_urbanrural[ab_popfile_urbanrural.urban_rural != 'U']

#Read in 10,000 rows of fbd_us_with_satellite_dec2015_v2.csv into DataFrame as natlbb_parsed. Clean DataFrame and Subset State/County FIPS from BlockCode.
file_natlbb = "fbd_us_with_satellite_dec2015_v2.csv"
columnNames_natlbb = ["LogRecNo", "Provider_ID", "FRN", "ProviderName", "DBAName", "HoldingCompany", "HocoNum", "HocoFinal", "StateAbbr", "BlockCode", "TechCode", "Consumer", "MaxAdDown", "MaxAdUp", "Business", "MaxCIRDown", "MaxCIRUp"]
natlbb_parsed = pd.read_csv(file_natlbb, delimiter=",", names=columnNames_natlbb, skiprows=1, dtype={'BlockCode':str}, nrows=10000, encoding='latin-1')
natlbb_parsed['BlockCode'] = natlbb_parsed['BlockCode'].str[:15]
natlbb_parsed['ProviderName'] = natlbb_parsed['ProviderName'].str[:23]
natlbb_parsed['DBAName'] = natlbb_parsed['DBAName'].str[:23]
natlbb_parsed['MaxAdDown'] = natlbb_parsed['MaxAdDown'].apply(int)
natlbb_parsed['MaxAdUp'] = natlbb_parsed['MaxAdUp'].apply(int)
natlbb_parsed['MaxCIRUp'] = natlbb_parsed['MaxCIRUp'].apply(int)
natlbb_parsed['MaxCIRDown'] = natlbb_parsed['MaxCIRDown'].apply(int)
natlbb_parsed_sorted = natlbb_parsed.sort_values('BlockCode')
stateFIPS = natlbb_parsed.BlockCode.str.slice(0, 2)
stateFIPS = pd.Series.to_frame(stateFIPS)
stateFIPS = stateFIPS.rename(columns={'BlockCode': 'stateFIPS'})
countyFIPS = natlbb_parsed.BlockCode.str.slice(0, 5)
countyFIPS = pd.Series.to_frame(countyFIPS)
countyFIPS = countyFIPS.rename(columns={'BlockCode': 'countyFIPS'})
natlbb_parsed = pd.concat([natlbb_parsed, stateFIPS, countyFIPS], axis=1)

#Read in County Names
file_county_data = "FIPScodesAndName2010.csv"
county_names = pd.read_csv(file_county_data, delimiter=",", nrows=10000)
county_names = county_names.rename(columns={county_names.columns[0]: 'STATE',
                                            county_names.columns[1]: 'STATEFP',
                                            county_names.columns[2]: 'COUNTYFP',
                                            county_names.columns[3]: 'COUNTYNAME',
                                            county_names.columns[4]: 'CLASSFP'
                                            })
county_names.to_csv(os.path.join(workdir, r'FIPScodesAndName2010_headers_dtype.csv'), encoding='utf-8')
county_names_formatted = pd.read_csv(r'FIPScodesAndName2010_headers_dtype.csv', delimiter=",", nrows=10000, dtype={'STATE'     : str,
                                                                                                                   'COUNTYFP'  : int,
                                                                                                                   'STATEFP'   : int,
                                                                                                                   })
county_names_formatted.COUNTYFP = county_names_formatted.COUNTYFP.apply(lambda x: '{0:0>3}'.format(x))
county_names_formatted.STATEFP = county_names_formatted.STATEFP.apply(lambda x: '{0:0>2}'.format(x))
county_names_formatted['countyFIPS'] = county_names_formatted['STATEFP'] + county_names_formatted['COUNTYFP']

#Read in County Level Geolytics, Ensure Common Datatypes, Join County Names and Population Data on FIPS, Sort by FIPS.
file_county_geolytics = "county_geolytics2014.csv"
columnNames_county_geolytics = ["countyFIPS", "Population", "Housing_Units"]
geolytics_county_parsed = pd.read_csv(file_county_geolytics, delimiter=",", names=columnNames_county_geolytics, skiprows=1)
geolytics_county_parsed['countyFIPS'] = geolytics_county_parsed['countyFIPS'].apply(int)
county_names_formatted['countyFIPS'] = county_names_formatted['countyFIPS'].apply(int)
county_data = county_names_formatted.merge(geolytics_county_parsed, how="inner", on="countyFIPS")
county_data = county_data.sort_values("countyFIPS").drop('Unnamed: 0', axis=1)
county_data = county_data.rename(columns={'countyFIPS':'countyFIPS'})

#Join National Broadband Sample and Population by BlockCode
##TODO - Remove sampe .csv's below that are used for testing
natlbb_pop = natlbb_parsed.merge(geolytics2014_parsed, how ="inner", on="BlockCode")
natlbb_pop.loc[:'gl_pop'] *= 1
natlbb_pop.to_csv(os.path.join(workdir, r'NatlbbWithPopulation.csv'), encoding='utf-8')
natlbb_pop = pd.read_csv('NatlbbWithPopulation2.csv', delimiter=',', encoding='utf-8')

#Table 1 - Unique Block Groups with Population, County and State FIPS. Drop Duplicates and Reset Index to Increment by 1. Make Columns Explicit.
BlockGroupPop = pd.concat([natlbb_pop['stateFIPS'], natlbb_pop['BlockCode'], natlbb_pop['countyFIPS'], natlbb_pop['gl_pop']], axis=1)
BlockGroupPop = BlockGroupPop.drop_duplicates()
BlockGroupPop = BlockGroupPop.reset_index()
BlockGroupPop = BlockGroupPop.drop('index', axis=1).rename(columns={'gl_pop': 'BlockGroupPop'})

#Table 2 - County Populations
CountyPop = BlockGroupPop.groupby(['countyFIPS', 'stateFIPS'])['BlockGroupPop'].sum().reset_index().rename(columns={'BlockGroupPop': 'CountyPop'})
CountyPop = CountyPop.reset_index()
CountyPop = CountyPop.drop('index', axis=1)

#Table 3 - State Populations
StatePop = CountyPop.groupby(['stateFIPS'])['CountyPop'].sum().reset_index().rename(columns={'CountyPop': 'StatePop'})


#Table 4 - County and State Populations
CountyAndStatePop = CountyPop.merge(StatePop, how='inner', on='stateFIPS')

#Table 5 - National Population
NationalPop = StatePop['StatePop'].sum()

#Table 6 - Merge State and County Population Data with Broadband Deployment Data
BB_Pop = natlbb_pop.merge(CountyAndStatePop, how='inner', on='countyFIPS')
BB_Pop = BB_Pop.rename(columns={'stateFIPS_y': 'stateFIPS'})
BB_Pop = BB_Pop.drop('stateFIPS_x',axis=1)

#Table 7 - Merge County Data and Population Data
county_data = county_data.merge(BB_Pop, how='inner', on='countyFIPS')

#Table 8 - Block Groups as Percent of County, State and National Populations
bb_avail_all_pop = BB_Pop.copy()
bb_avail_all_pop['blockpop_pct_of_county'] = 10*(bb_avail_all_pop['gl_pop']/bb_avail_all_pop['CountyPop'])
bb_avail_all_pop['blockpop_pct_of_state'] = 10*(bb_avail_all_pop['gl_pop']/bb_avail_all_pop['StatePop'])
bb_avail_all_pop['blockpop_pct_of_nation'] = 10*(bb_avail_all_pop['gl_pop']/NationalPop)
bb_avail_all_pop = bb_avail_all_pop[bb_avail_all_pop.gl_pop != 0]

#Table 9 - Percent of Block Code Population with Broadband Access
bb_access_bc = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc = bb_access_bc[bb_access_bc.MaxAdUp >= 3]
bb_access_bc = bb_access_bc[bb_access_bc.MaxAdDown >=25]

#Table 10 - Percent of County Population with Broadband Access
bb_access_c = pd.Series.to_frame(bb_access_bc.groupby(['countyFIPS'])['blockpop_pct_of_county'].sum())
bb_access_c = bb_access_c.rename(columns={'blockpop_pct_of_county':'pctpopwBBacc_county'})
bb_access_c['countyFIPS'] = bb_access_c.index

#Table 11 - Percent of State Population with Broadband Access
bb_access_s = pd.Series.to_frame(bb_access_bc.groupby(['stateFIPS'])['blockpop_pct_of_state'].sum())
bb_access_s = bb_access_s.rename(columns={'blockpop_pct_of_state':'pctpopwBBacc_state'})
bb_access_s['stateFIPS'] = bb_access_s.index

#Table 12 - Percent of National Population with Broadband Access
bb_access_n = pd.Series.to_frame(bb_access_bc.groupby(['stateFIPS'])['blockpop_pct_of_nation'].sum())
bb_access_n = bb_access_n.rename(columns={'blockpop_pct_of_nation':'pctpopwBBacc_nat'})
bb_access_n['stateFIPS'] = bb_access_n.index

#Table 13 - Percent of Block Group Population with Broadband Download Speed Access >25Mbps
bb_access_bc_ds = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc_ds = bb_access_bc[bb_access_bc.MaxAdDown >= 25]

#Table 14 - Percent of County Population with Broadband Download Speed Access >25Mbps
bb_access_c_ds = pd.Series.to_frame(bb_access_bc_ds.groupby(['countyFIPS'])['blockpop_pct_of_county'].sum())
bb_access_c_ds = bb_access_c_ds.rename(columns={'blockpop_pct_of_county':'pctpopwBBds_county'})
bb_access_c_ds['countyFIPS'] = bb_access_c_ds.index

#Table 15 - Percent of State Population with Broadband Download Speed Access >25Mbps
bb_access_s_ds = pd.Series.to_frame(bb_access_bc_ds.groupby(['stateFIPS'])['blockpop_pct_of_state'].sum())
bb_access_s_ds = bb_access_s_ds.rename(columns={'blockpop_pct_of_state':'pctpopwBBds_state'})
bb_access_s_ds['stateFIPS'] = bb_access_s_ds.index

#Table 16 - Percent of National Population with Broadband Download Speed Access >25Mbps
bb_access_n_ds = pd.Series.to_frame(bb_access_bc.groupby(['stateFIPS'])['blockpop_pct_of_nation'].sum())
bb_access_n_ds = bb_access_n_ds.rename(columns={'blockpop_pct_of_nation':'pctpopwBBds_nat'})
bb_access_n_ds['stateFIPS'] = bb_access_n_ds.index

#Table 17 - Percent of Block Group Population with Broadband Upload Speed Access >3Mbps
bb_access_bc_us = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc_us = bb_access_bc[bb_access_bc.MaxAdUp >= 3]

#Table 18 - Percent of County Population with Broadband Upload Speed Access >3Mbps
bb_access_c_us = pd.Series.to_frame(bb_access_bc_us.groupby(['countyFIPS'])['blockpop_pct_of_county'].sum())
bb_access_c_us = bb_access_c_us.rename(columns={'blockpop_pct_of_county':'pctpopwBBus_county'})
bb_access_c_us['countyFIPS'] = bb_access_c_us.index

#Table 19 - Percent of State Population with Broadband Upload Speed Access >3Mbps
bb_access_s_us = pd.Series.to_frame(bb_access_bc_ds.groupby(['stateFIPS'])['blockpop_pct_of_state'].sum())
bb_access_s_us = bb_access_s_us.rename(columns={'blockpop_pct_of_state':'pctpopwBBus_state'})
bb_access_s_us['stateFIPS'] = bb_access_s_us.index

#Table 20 - Percent of National Population with Broadband Upload Speed Access >3Mbps
bb_access_n_us = pd.Series.to_frame(bb_access_bc.groupby(['stateFIPS'])['blockpop_pct_of_nation'].sum())
bb_access_n_us = bb_access_n_us.rename(columns={'blockpop_pct_of_nation':'pctpopwBBus_nat'})
bb_access_n_us['stateFIPS'] = bb_access_n_us.index

#Sort Tables by FIPS
county_data = county_data.sort_values('countyFIPS')
bb_access_c =  bb_access_c.sort_values('countyFIPS')
bb_access_s =  bb_access_s.sort_values('stateFIPS')
bb_access_n =  bb_access_n.sort_values('stateFIPS')
bb_access_c_ds =  bb_access_c_ds.sort_values('countyFIPS')
bb_access_s_ds =  bb_access_s_ds.sort_values('stateFIPS')
bb_access_n_ds =  bb_access_n_ds.sort_values('stateFIPS')
bb_access_c_us =  bb_access_c_us.sort_values('countyFIPS')
bb_access_s_us =  bb_access_s_us.sort_values('stateFIPS')
bb_access_n_us =  bb_access_n_us.sort_values('stateFIPS')

#Merge County Name and Population Data with Broadband Access Percentages
county_data = county_data.join(bb_access_c['pctpopwBBacc_county'], how='inner', on='countyFIPS')
county_data = county_data.join(bb_access_c_ds['pctpopwBBds_county'], how='inner', on='countyFIPS')
county_data = county_data.join(bb_access_c_us['pctpopwBBus_county'], how='inner', on='countyFIPS')
county_data = county_data.join(bb_access_s['pctpopwBBacc_state'], how='inner', on='stateFIPS')
county_data = county_data.join(bb_access_n['pctpopwBBacc_nat'], how='inner', on='stateFIPS')
county_data = county_data.join(bb_access_s_ds['pctpopwBBds_state'], how='inner', on='stateFIPS')
county_data = county_data.join(bb_access_n_ds['pctpopwBBds_nat'], how='inner', on='stateFIPS')
county_data = county_data.join(bb_access_s_us['pctpopwBBus_state'], how='inner', on='stateFIPS')
county_data = county_data.join(bb_access_n_us['pctpopwBBus_nat'], how='inner', on='stateFIPS')

#Get County, State and National Percent Without Broadband and Join Them on FIPS
pctpopwoBBacc_county = pd.Series.to_frame(100-county_data['pctpopwBBacc_county']).rename(columns={'pctpopwBBacc_county':'pctpopwoBBacc_county'})
pctpopwoBBacc_state = pd.Series.to_frame(100-county_data['pctpopwBBacc_state']).rename(columns={'pctpopwBBacc_state':'pctpopwoBBacc_state'})
pctpopwoBBacc_nat = pd.Series.to_frame(100-county_data['pctpopwBBacc_nat']).rename(columns={'pctpopwBBacc_nat':'pctpopwoBBacc_nat'})
county_data = county_data.join(pctpopwoBBacc_county, how='inner',on='countyFIPS')
county_data = county_data.join(pctpopwoBBacc_state, how='inner', on='stateFIPS')
county_data = county_data.join(pctpopwoBBacc_nat, how='inner', on='stateFIPS')

#Table 21 -Create Table of Consumers and Remove Records where Consumer != 1
ConsumerProviders = pd.concat([bb_avail_all_pop['stateFIPS'],
                               bb_avail_all_pop['countyFIPS'],
                               bb_avail_all_pop['BlockCode'],
                               bb_avail_all_pop['Consumer'],
                               bb_avail_all_pop['MaxAdDown'],
                               bb_avail_all_pop['MaxAdUp'],
                               bb_avail_all_pop['blockpop_pct_of_county'],
                               bb_avail_all_pop['blockpop_pct_of_state'],
                               bb_avail_all_pop['blockpop_pct_of_nation'],
                               bb_avail_all_pop['gl_pop'],
                               bb_avail_all_pop['CountyPop']], axis=1)

ConsumerProviders = ConsumerProviders.query("Consumer == 1")[['stateFIPS','countyFIPS','BlockCode','MaxAdDown','MaxAdUp','blockpop_pct_of_county','blockpop_pct_of_state','blockpop_pct_of_nation','gl_pop','CountyPop']]
highestSpeedinBC_DL = idx = ConsumerProviders.groupby([])


getval = geolytics2014_parsed.query("BlockCode == 080099647001143")[['gl_pop']]