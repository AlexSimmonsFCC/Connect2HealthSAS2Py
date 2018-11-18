import pandas as pd
import signal
import os
import sys
import numpy
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'

#Read in geolytics.csv into DataFrame as 'file_geolytics2016'
geolytics2016_parsed = pd.read_csv(r'us2016.csv', usecols = [0,1,8,15,22]).rename(columns={'block_fips':'BlockCode','pop2016':'gl_pop','hu2016':'gl_housingunits','hh2016':'gl_households'})
geolytics2016_parsed['BlockCode'] = geolytics2016_parsed['BlockCode'].apply('{:0>15}'.format)

#Read in fbd_us_with_satellite_dec2015_v2.csv into DataFrame as natlbb_parsed. Clean DataFrame and Subset State/County FIPS from BlockCode.
natlbb_parsed = pd.read_csv(r'fbd_us_with_satellite_dec2016_v1.csv', delimiter=",", encoding='latin-1', dtype={'LogRecNo': int, 'Provider_Id': int, 'FRN': int, 'HocoNum': int, 'StateAbbr': str, 'BlockCode': int, 'TechCode': int,'Consumer': int, 'MaxAdDown': int, 'MaxAdUp': int, 'Business': int, 'MaxCIRDown': int, 'MaxCIRUp': int}, engine='python')
natlbb_parsed['DBAName'] = natlbb_parsed['DBAName'].str[:23]
natlbb_parsed['BlockCode'] = natlbb_parsed['BlockCode'].apply('{:0>15}'.format)
natlbb_parsed['stateFIPS'] = pd.to_numeric(natlbb_parsed['BlockCode'].astype(str).str[:2])
natlbb_parsed['countyFIPS'] = pd.to_numeric(natlbb_parsed['BlockCode'].astype(str).str[:5])

#Join National Broadband Sample and Population by BlockCode
natlbb_pop = natlbb_parsed.merge(geolytics2016_parsed, how ="inner", on="BlockCode")
natlbb_pop['gl_pop'].fillna(0, inplace=True)
natlbb_pop['gl_pop'] = natlbb_pop['gl_pop'].astype(int)
natlbb_pop.to_csv(os.path.join(workdir, r'NatlbbWithPopulation.csv'), encoding = 'latin-1')
natlbb_pop = pd.read_csv('NatlbbWithPopulation.csv', encoding='latin-1',usecols=['stateFIPS','countyFIPS','BlockCode','Consumer','MaxAdDown','MaxAdUp','gl_pop','gl_housingunits','gl_households'])
county_names_formatted = pd.read_csv('FIPScodesAndName2010_headers_dtype.csv')

#Read in County Level Geolytics, Ensure Common Datatypes, Join County Names and Population Data on FIPS, Sort by FIPS.
file_county_geolytics = "county_FCC.csv"
columnNames_county_geolytics = ["countyFIPS", "Population", "Housing_Units"]
geolytics_county_parsed = pd.read_csv(file_county_geolytics, delimiter=",", names=columnNames_county_geolytics, skiprows=1)
geolytics_county_parsed['countyFIPS'] = geolytics_county_parsed['countyFIPS'].apply(lambda x: '{0:0>5}'.format(x))
county_names_formatted['COUNTYFP'] = county_names_formatted['COUNTYFP'].apply(lambda x: '{0:0>3}'.format(x))
county_names_formatted['countyFIPS'] = county_names_formatted['STATEFP'].astype(str) + county_names_formatted['COUNTYFP'].astype(str)
county_names_formatted = county_names_formatted.rename(columns={'STATEFP':'stateFIPS'})
county_names_formatted['countyFIPS'] = county_names_formatted['countyFIPS'].apply('{:0>5}'.format)
county_names_formatted.to_csv(os.path.join(workdir, r'county_names_formatted_test.csv'), encoding = 'latin-1')
geolytics_county_parsed['countyFIPS'] = geolytics_county_parsed['countyFIPS'].astype(str)
county_data = county_names_formatted.merge(geolytics_county_parsed, how="inner", on="countyFIPS")
county_data['stateFIPS'] = county_data['stateFIPS'].apply('{:0>2}'.format)


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
#natlbb_pop_county = natlbb_pop.groupby(['countyFIPS'], as_index=False)['gl_pop'].sum()
#natlbb_pop_county.to_csv(os.path.join(workdir, r'natlbb_pop_county_test.csv'), encoding='latin-1')
#county_data.to_csv(os.path.join(workdir, r'county_data_table_7.csv'), encoding='latin-1')
#county_data = county_data.merge(natlbb_pop_county, how='inner', on='countyFIPS')

#Table 8 - Block Groups as Percent of County, State and National Populations
bb_avail_all_pop = BB_Pop.copy()
bb_avail_all_pop['blockpop_pct_of_county'] = 100*(bb_avail_all_pop['gl_pop']/bb_avail_all_pop['CountyPop'])
bb_avail_all_pop['blockpop_pct_of_state'] = 100*(bb_avail_all_pop['gl_pop']/bb_avail_all_pop['StatePop'])
bb_avail_all_pop['blockpop_pct_of_nation'] = 100*(bb_avail_all_pop['gl_pop']/NationalPop)
bb_avail_all_pop = bb_avail_all_pop[bb_avail_all_pop.gl_pop != 0]

#Table 9 - Percent of Block Code Population with Broadband Access
bb_access_bc_no_acesss = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc_no_acesss = bb_access_bc_no_acesss[bb_access_bc_no_acesss.MaxAdUp <= 3]
bb_access_bc_no_acesss = bb_access_bc_no_acesss[bb_access_bc_no_acesss.MaxAdDown <=25]
bb_access_bc_no_acesss['blockpop_pct_of_county'] = 0
bb_access_bc_no_acesss['blockpop_pct_of_state'] = 0
bb_access_bc_no_acesss['blockpop_pct_of_nation'] = 0

bb_access_bc = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc = bb_access_bc[bb_access_bc.MaxAdUp >= 3]
bb_access_bc = bb_access_bc[bb_access_bc.MaxAdDown >=25]
bb_access_bc = bb_access_bc[['stateFIPS','countyFIPS','BlockCode','blockpop_pct_of_county','blockpop_pct_of_state','blockpop_pct_of_nation']]
bb_access_bc = bb_access_bc.drop_duplicates()

bb_access_bc = bb_access_bc.append(bb_access_bc_no_acesss, ignore_index=True)


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
bb_access_bc_ds_no_access = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc_ds_no_access = bb_access_bc_ds_no_access[bb_access_bc_ds_no_access.MaxAdDown >= 25]
bb_access_bc_ds_no_access['blockpop_pct_of_county'] = 0
bb_access_bc_ds_no_access['blockpop_pct_of_state'] = 0
bb_access_bc_ds_no_access['blockpop_pct_of_nation'] = 0


bb_access_bc_ds = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc_ds = bb_access_bc_ds[bb_access_bc_ds.MaxAdDown >= 25]
bb_access_bc_ds = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation']], axis=1)
bb_access_bc_ds = bb_access_bc_ds.drop_duplicates()

bb_access_bc_ds = bb_access_bc_ds.append(bb_access_bc_no_acesss, ignore_index=True)


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
bb_access_bc_us_no_access  = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc_us_no_access = bb_access_bc_us_no_access[bb_access_bc_us_no_access.MaxAdUp <= 3]
bb_access_bc_us_no_access['blockpop_pct_of_county'] = 0
bb_access_bc_us_no_access['blockpop_pct_of_state'] = 0
bb_access_bc_us_no_access['blockpop_pct_of_nation'] = 0


bb_access_bc_us = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation'],bb_avail_all_pop['MaxAdDown'],bb_avail_all_pop['MaxAdUp']], axis=1)
bb_access_bc_us = bb_access_bc_us[bb_access_bc_us.MaxAdUp >= 3]
bb_access_bc_us = pd.concat([bb_avail_all_pop['stateFIPS'], bb_avail_all_pop['countyFIPS'],bb_avail_all_pop['BlockCode'],bb_avail_all_pop['blockpop_pct_of_county'],bb_avail_all_pop['blockpop_pct_of_state'],bb_avail_all_pop['blockpop_pct_of_nation']], axis=1)
bb_access_bc_us = bb_access_bc_us.drop_duplicates()

bb_access_bc_us = bb_access_bc_us.append(bb_access_bc_us_no_access)


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


#Merge County Name and Population Data with Broadband Access Percentages
bb_access_c['countyFIPS'] = bb_access_c['countyFIPS'].astype(str)
bb_access_c_ds['countyFIPS'] = bb_access_c_ds['countyFIPS'].astype(str)
bb_access_c_us['countyFIPS'] = bb_access_c_us['countyFIPS'].astype(str)
bb_access_n['pctpopwBBacc_nat'] = bb_access_n['pctpopwBBacc_nat'].sum()
bb_access_n_ds['pctpopwBBds_nat'] = bb_access_n_ds['pctpopwBBds_nat'].sum()
bb_access_n_us['pctpopwBBus_nat'] = bb_access_n_us['pctpopwBBus_nat'].sum()

bb_access_c['countyFIPS'] = bb_access_c['countyFIPS'].apply('{:0>5}'.format)
bb_access_c_ds['countyFIPS'] = bb_access_c_ds['countyFIPS'].apply('{:0>5}'.format)
bb_access_c_us['countyFIPS'] = bb_access_c_us['countyFIPS'].apply('{:0>5}'.format)
bb_access_s['stateFIPS'] = bb_access_s['stateFIPS'].apply('{:0>2}'.format)
bb_access_n['stateFIPS'] = bb_access_n['stateFIPS'].apply('{:0>2}'.format)
bb_access_s_ds['stateFIPS'] = bb_access_s_ds['stateFIPS'].apply('{:0>2}'.format)
bb_access_n_ds['stateFIPS'] = bb_access_n_ds['stateFIPS'].apply('{:0>2}'.format)
bb_access_s_us['stateFIPS'] = bb_access_s_us['stateFIPS'].apply('{:0>2}'.format)
bb_access_n_us['stateFIPS'] = bb_access_n_us['stateFIPS'].apply('{:0>2}'.format)

county_data =  county_data.merge(bb_access_c, how='inner', on='countyFIPS')
county_data =  county_data.merge(bb_access_c_ds, how='inner', on='countyFIPS')
county_data =  county_data.merge(bb_access_c_us, how='inner', on='countyFIPS')
county_data =  county_data.merge(bb_access_s, how='inner', on='stateFIPS')
county_data =  county_data.merge(bb_access_n, how='inner', on='stateFIPS')
county_data =  county_data.merge(bb_access_s_ds, how='inner', on='stateFIPS')
county_data =  county_data.merge(bb_access_n_ds, how='inner', on='stateFIPS')
county_data =  county_data.merge(bb_access_s_us, how='inner', on='stateFIPS')
county_data =  county_data.merge(bb_access_n_us, how='inner', on='stateFIPS')

#Get County, State and National Percent Without Broadband and Add Them to county_data
county_data['pctpopwoBBacc_county'] = 100 -county_data['pctpopwBBacc_county']
county_data['pctpopwoBBacc_state'] = 100 -county_data['pctpopwBBacc_state']
county_data['pctpopwoBBacc_nat'] = 100 -county_data['pctpopwBBacc_nat']

bb_avail_all_pop.to_csv(os.path.join('bb_avail_all_pop.csv'))

#Table 21 -Create Table of Consumers and Remove Records where Consumer != 1
ConsumerProviders = bb_avail_all_pop
ConsumerProviders = ConsumerProviders.query("Consumer == 1")[['stateFIPS','countyFIPS','BlockCode','MaxAdDown','MaxAdUp','blockpop_pct_of_county','blockpop_pct_of_state','blockpop_pct_of_nation','gl_pop','CountyPop']]


## WRITE OUT TABLES FOR NEXT STEP **
county_data.to_csv(os.path.join(workdir, r'countynameswithpcts.csv'), encoding='latin-1')
ConsumerProviders.to_csv(os.path.join(workdir, r'ResultConsumerProviders.csv'), encoding='latin-1')

import pandas as pd
import signal
import os
import numpy
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'


#Table 22 - Create Table of Download Speed Tiers and Assign County Percent of Population to each Tier
ConsumerProviders = pd.read_csv(r'ResultConsumerProviders.csv')
ConsumerProviders = ConsumerProviders[['stateFIPS','countyFIPS','BlockCode','MaxAdDown','blockpop_pct_of_county','blockpop_pct_of_state','blockpop_pct_of_nation','gl_pop','CountyPop']]
county_data = pd.read_csv(r'countynameswithpcts.csv')
county_data['merge_level'] = 'national'
idx = ConsumerProviders.groupby(['BlockCode'])['MaxAdDown'].transform(max) == ConsumerProviders['MaxAdDown']
ConsumerProviders = ConsumerProviders[idx]
ConsumerProviders = ConsumerProviders.drop_duplicates()

highestSpeedinBC_DL = ConsumerProviders.copy()
highestSpeedinBC_DL['merge_level'] = 'national'
highestSpeedinBC_DL['DS0_hi_c'] = ""
highestSpeedinBC_DL['DS0_hi_s'] = ""
highestSpeedinBC_DL['DS0_hi_n'] = ""
highestSpeedinBC_DL['DSGt0kAndLt1000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt0kAndLt1000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt0kAndLt1000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt1000kAndLt3000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt1000kAndLt3000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt1000kAndLt3000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt3000kAndLt4000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt3000kAndLt4000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt3000kAndLt4000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt4000kAndLt6000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt4000kAndLt6000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt4000kAndLt6000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt6000kAndLt10000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt6000kAndLt10000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt6000kAndLt10000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt10000kAndLt15000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt10000kAndLt15000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt10000kAndLt15000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt15000kAndLt25000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt15000kAndLt25000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt15000kAndLt25000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt25000kAndLt50000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt25000kAndLt50000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt25000kAndLt50000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt50000kAndLt100000k_hi_c'] = ""
highestSpeedinBC_DL['DSGt50000kAndLt100000k_hi_s'] = ""
highestSpeedinBC_DL['DSGt50000kAndLt100000k_hi_n'] = ""
highestSpeedinBC_DL['DSGt100000kAndLt1Gig_hi_c'] = ""
highestSpeedinBC_DL['DSGt100000kAndLt1Gig_hi_s'] = ""
highestSpeedinBC_DL['DSGt100000kAndLt1Gig_hi_n'] = ""
highestSpeedinBC_DL['DSGt1Gig_hi_c'] = ""
highestSpeedinBC_DL['DSGt1Gig_hi_s'] = ""
highestSpeedinBC_DL['DSGt1Gig_hi_n'] = ""
highestSpeedinBC_DL.DS0_hi_c = ConsumerProviders.blockpop_pct_of_county.where(ConsumerProviders.MaxAdDown == 0)
highestSpeedinBC_DL.DS0_hi_s = ConsumerProviders.blockpop_pct_of_state.where(ConsumerProviders.MaxAdDown == 0)
highestSpeedinBC_DL.DS0_hi_n = ConsumerProviders.blockpop_pct_of_nation.where(ConsumerProviders.MaxAdDown == 0)
highestSpeedinBC_DL.DSGt0kAndLt1000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown > 0) & (ConsumerProviders.MaxAdDown < 1))
highestSpeedinBC_DL.DSGt0kAndLt1000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown > 0) & (ConsumerProviders.MaxAdDown < 1))
highestSpeedinBC_DL.DSGt0kAndLt1000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown > 0) & (ConsumerProviders.MaxAdDown < 1))
highestSpeedinBC_DL.DSGt1000kAndLt3000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 1) & (ConsumerProviders.MaxAdDown < 3))
highestSpeedinBC_DL.DSGt1000kAndLt3000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 1) & (ConsumerProviders.MaxAdDown < 3))
highestSpeedinBC_DL.DSGt1000kAndLt3000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 1) & (ConsumerProviders.MaxAdDown < 3))
highestSpeedinBC_DL.DSGt3000kAndLt4000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 3) & (ConsumerProviders.MaxAdDown < 4))
highestSpeedinBC_DL.DSGt3000kAndLt4000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 3) & (ConsumerProviders.MaxAdDown < 4))
highestSpeedinBC_DL.DSGt3000kAndLt4000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 3) & (ConsumerProviders.MaxAdDown < 4))
highestSpeedinBC_DL.DSGt4000kAndLt6000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 4) & (ConsumerProviders.MaxAdDown < 6))
highestSpeedinBC_DL.DSGt4000kAndLt6000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 4) & (ConsumerProviders.MaxAdDown < 6))
highestSpeedinBC_DL.DSGt4000kAndLt6000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 4) & (ConsumerProviders.MaxAdDown < 6))
highestSpeedinBC_DL.DSGt6000kAndLt10000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 6) & (ConsumerProviders.MaxAdDown < 10))
highestSpeedinBC_DL.DSGt6000kAndLt10000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 6) & (ConsumerProviders.MaxAdDown < 10))
highestSpeedinBC_DL.DSGt6000kAndLt10000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 6) & (ConsumerProviders.MaxAdDown < 10))
highestSpeedinBC_DL.DSGt10000kAndLt15000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 10) & (ConsumerProviders.MaxAdDown < 15))
highestSpeedinBC_DL.DSGt10000kAndLt15000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 10) & (ConsumerProviders.MaxAdDown < 15))
highestSpeedinBC_DL.DSGt10000kAndLt15000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 10) & (ConsumerProviders.MaxAdDown < 15))
highestSpeedinBC_DL.DSGt15000kAndLt25000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 15) & (ConsumerProviders.MaxAdDown < 25))
highestSpeedinBC_DL.DSGt15000kAndLt25000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 15) & (ConsumerProviders.MaxAdDown < 25))
highestSpeedinBC_DL.DSGt15000kAndLt25000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 15) & (ConsumerProviders.MaxAdDown < 25))
highestSpeedinBC_DL.DSGt25000kAndLt50000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 25) & (ConsumerProviders.MaxAdDown < 50))
highestSpeedinBC_DL.DSGt25000kAndLt50000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 25) & (ConsumerProviders.MaxAdDown < 50))
highestSpeedinBC_DL.DSGt25000kAndLt50000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 25) & (ConsumerProviders.MaxAdDown < 50))
highestSpeedinBC_DL.DSGt50000kAndLt100000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 50) & (ConsumerProviders.MaxAdDown < 100))
highestSpeedinBC_DL.DSGt50000kAndLt100000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 50) & (ConsumerProviders.MaxAdDown < 100))
highestSpeedinBC_DL.DSGt50000kAndLt100000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 50) & (ConsumerProviders.MaxAdDown < 100))
highestSpeedinBC_DL.DSGt100000kAndLt1Gig_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 100) & (ConsumerProviders.MaxAdDown < 1000))
highestSpeedinBC_DL.DSGt100000kAndLt1Gig_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 100) & (ConsumerProviders.MaxAdDown < 1000))
highestSpeedinBC_DL.DSGt100000kAndLt1Gig_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 100) & (ConsumerProviders.MaxAdDown < 1000))
highestSpeedinBC_DL.DSGt1Gig_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdDown >= 1000))
highestSpeedinBC_DL.DSGt1Gig_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdDown >= 1000))
highestSpeedinBC_DL.DSGt1Gig_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdDown >= 1000))

#Table 23 - Sum Download Speed Tier Percentages of Counties
DLspeed_tiers_hi_c = highestSpeedinBC_DL.groupby(['countyFIPS'])['DS0_hi_c','DSGt0kAndLt1000k_hi_c','DSGt1000kAndLt3000k_hi_c','DSGt3000kAndLt4000k_hi_c','DSGt4000kAndLt6000k_hi_c','DSGt6000kAndLt10000k_hi_c','DSGt10000kAndLt15000k_hi_c','DSGt15000kAndLt25000k_hi_c','DSGt25000kAndLt50000k_hi_c','DSGt50000kAndLt100000k_hi_c','DSGt100000kAndLt1Gig_hi_c','DSGt1Gig_hi_c'].sum()
DLspeed_tiers_hi_c['countyFIPS'] = DLspeed_tiers_hi_c.index
DLspeed_tiers_hi_c['countyFIPS'] = DLspeed_tiers_hi_c['countyFIPS'].astype(int)
DLspeed_tiers_hi_c = DLspeed_tiers_hi_c.rename(columns={'DS0_hi_c':'pctDS0_hi_c','DSGt0kAndLt1000k_hi_c':'pctDSGt0kAndLt1000k_hi_c','DSGt1000kAndLt3000k_hi_c':'pctDSGt1000kAndLt3000k_hi_c','DSGt4000kAndLt6000k_hi_c':'pctDSGt4000kAndLt6000k_hi_c','DSGt6000kAndLt10000k_hi_c':'pctDSGt6000kAndLt10000k_hi_c','DSGt10000kAndLt15000k_hi_c':'pctDSGt10000kAndLt15000k_hi_c','DSGt15000kAndLt25000k_hi_c':'pctDSGt15000kAndLt25000k_hi_c','DSGt25000kAndLt50000k_hi_c':'pctDSGt25000kAndLt50000k_hi_c','DSGt50000kAndLt100000k_hi_c':'pctDSGt50000kAndLt100000k_hi_c','DSGt100000kAndLt1Gig_hi_c':'pctDSGt100000kAndLt1Gig_hi_c','DSGt1Gig_hi_c':'pctDSGt1Gig_hi_c','DSGt3000kAndLt4000k_hi_c':'pctDSGt3000kAndLt4000k_hi_c'})
DLspeed_tiers_hi_c = DLspeed_tiers_hi_c.fillna(0)


#Table 23 - Sum Download Speed Tier Percentages of States
DLspeed_tiers_hi_s = highestSpeedinBC_DL.groupby(['stateFIPS'])['DS0_hi_s','DSGt0kAndLt1000k_hi_s','DSGt1000kAndLt3000k_hi_s','DSGt3000kAndLt4000k_hi_s','DSGt4000kAndLt6000k_hi_s','DSGt6000kAndLt10000k_hi_s','DSGt10000kAndLt15000k_hi_s','DSGt15000kAndLt25000k_hi_s','DSGt25000kAndLt50000k_hi_s','DSGt50000kAndLt100000k_hi_s','DSGt100000kAndLt1Gig_hi_s','DSGt1Gig_hi_s'].sum()
DLspeed_tiers_hi_s['stateFIPS'] = DLspeed_tiers_hi_s.index
DLspeed_tiers_hi_s['stateFIPS'] = DLspeed_tiers_hi_s['stateFIPS'].astype(int)
DLspeed_tiers_hi_s = DLspeed_tiers_hi_s.rename(columns={'DS0_hi_s':'pctDS0_hi_s','DSGt0kAndLt1000k_hi_s':'pctDSGt0kAndLt1000k_hi_s','DSGt1000kAndLt3000k_hi_s':'pctDSGt1000kAndLt3000k_hi_s','DSGt4000kAndLt6000k_hi_s':'pctDSGt4000kAndLt6000k_hi_s','DSGt6000kAndLt10000k_hi_s':'pctDSGt6000kAndLt10000k_hi_s','DSGt10000kAndLt15000k_hi_s':'pctDSGt10000kAndLt15000k_hi_s','DSGt15000kAndLt25000k_hi_s':'pctDSGt15000kAndLt25000k_hi_s','DSGt25000kAndLt50000k_hi_s':'pctDSGt25000kAndLt50000k_hi_s','DSGt50000kAndLt100000k_hi_s':'pctDSGt50000kAndLt100000k_hi_s','DSGt100000kAndLt1Gig_hi_s':'pctDSGt100000kAndLt1Gig_hi_s','DSGt1Gig_hi_s':'pctDSGt1Gig_hi_s','DSGt3000kAndLt4000k_hi_s':'pctDSGt3000kAndLt4000k_hi_s'})
DLspeed_tiers_hi_s = DLspeed_tiers_hi_s.fillna(0)


#Table 24 - Sum Download Speed Tier Percentages of Nation
DLspeed_tiers_hi_n = highestSpeedinBC_DL.groupby(['merge_level'])['DS0_hi_n','DSGt0kAndLt1000k_hi_n','DSGt1000kAndLt3000k_hi_n','DSGt3000kAndLt4000k_hi_n','DSGt4000kAndLt6000k_hi_n','DSGt6000kAndLt10000k_hi_n','DSGt10000kAndLt15000k_hi_n','DSGt15000kAndLt25000k_hi_n','DSGt25000kAndLt50000k_hi_n','DSGt50000kAndLt100000k_hi_n','DSGt100000kAndLt1Gig_hi_n','DSGt1Gig_hi_n'].sum()
DLspeed_tiers_hi_n = DLspeed_tiers_hi_n.rename(columns={'DS0_hi_n':'pctDS0_hi_n','DSGt0kAndLt1000k_hi_n':'pctDSGt0kAndLt1000k_hi_n','DSGt1000kAndLt3000k_hi_n':'pctDSGt1000kAndLt3000k_hi_n','DSGt4000kAndLt6000k_hi_n':'pctDSGt4000kAndLt6000k_hi_n','DSGt6000kAndLt10000k_hi_n':'pctDSGt6000kAndLt10000k_hi_n','DSGt10000kAndLt15000k_hi_n':'pctDSGt10000kAndLt15000k_hi_n','DSGt15000kAndLt25000k_hi_n':'pctDSGt15000kAndLt25000k_hi_n','DSGt25000kAndLt50000k_hi_n':'pctDSGt25000kAndLt50000k_hi_n','DSGt50000kAndLt100000k_hi_n':'pctDSGt50000kAndLt100000k_hi_n','DSGt100000kAndLt1Gig_hi_n':'pctDSGt100000kAndLt1Gig_hi_n','DSGt1Gig_hi_n':'pctDSGt1Gig_hi_n','DSGt3000kAndLt4000k_hi_n':'pctDSGt3000kAndLt4000k_hi_n'})
DLspeed_tiers_hi_n['merge_level'] = 'national'
DLspeed_tiers_hi_n = DLspeed_tiers_hi_n.drop_duplicates()

DLspeed_tiers_hi_c['countyFIPS'] = DLspeed_tiers_hi_c['countyFIPS'].apply('{:0>5}'.format).astype(int)
DLspeed_tiers_hi_s['stateFIPS'] = DLspeed_tiers_hi_s['stateFIPS'].apply('{:0>2}'.format).astype(int)


DLspeed_tiers_hi_s.to_csv(os.path.join(workdir, r'DLspeed_tiers_hi_s.csv'), encoding='utf-8')
DLspeed_tiers_hi_c.to_csv(os.path.join(workdir, r'DLspeed_tiers_hi_c.csv'), encoding='utf-8')
DLspeed_tiers_hi_n.to_csv(os.path.join(workdir, r'DLspeed_tiers_hi_n.csv'), encoding='utf-8')

#Join County Data with County and State Download Speed Tiers
county_data = county_data.merge(DLspeed_tiers_hi_c,how='inner',on='countyFIPS')
county_data = county_data.merge(DLspeed_tiers_hi_s,how='inner',on='stateFIPS')
county_data = county_data.merge(DLspeed_tiers_hi_n,how='inner',on='merge_level')
county_data = county_data.drop_duplicates()

ConsumerProviders = pd.read_csv(r'ResultConsumerProviders.csv')
ConsumerProviders = ConsumerProviders[['stateFIPS','countyFIPS','BlockCode','MaxAdUp','blockpop_pct_of_county','blockpop_pct_of_state','blockpop_pct_of_nation','gl_pop','CountyPop']]
idx = ConsumerProviders.groupby(['BlockCode'])['MaxAdUp'].transform(max) == ConsumerProviders['MaxAdUp']
ConsumerProviders = ConsumerProviders[idx]
ConsumerProviders = ConsumerProviders.drop_duplicates()

#Table 24 - Create Table of Upload Speed Tiers and Assign County Percent of Population to each Tier
highestSpeedinBC_UL = ConsumerProviders.copy()
highestSpeedinBC_UL['merge_level'] = 'national'
highestSpeedinBC_UL['US0_hi_c'] = ""
highestSpeedinBC_UL['US0_hi_s'] = ""
highestSpeedinBC_UL['US0_hi_n'] = ""
highestSpeedinBC_UL['USGt0kAndLt1000k_hi_c'] = ""
highestSpeedinBC_UL['USGt0kAndLt1000k_hi_s'] = ""
highestSpeedinBC_UL['USGt0kAndLt1000k_hi_n'] = ""
highestSpeedinBC_UL['USGt1000kAndLt3000k_hi_c'] = ""
highestSpeedinBC_UL['USGt1000kAndLt3000k_hi_s'] = ""
highestSpeedinBC_UL['USGt1000kAndLt3000k_hi_n'] = ""
highestSpeedinBC_UL['USGt3000kAndLt4000k_hi_c'] = ""
highestSpeedinBC_UL['USGt3000kAndLt4000k_hi_s'] = ""
highestSpeedinBC_UL['USGt3000kAndLt4000k_hi_n'] = ""
highestSpeedinBC_UL['USGt4000kAndLt6000k_hi_c'] = ""
highestSpeedinBC_UL['USGt4000kAndLt6000k_hi_s'] = ""
highestSpeedinBC_UL['USGt4000kAndLt6000k_hi_n'] = ""
highestSpeedinBC_UL['USGt6000kAndLt10000k_hi_c'] = ""
highestSpeedinBC_UL['USGt6000kAndLt10000k_hi_s'] = ""
highestSpeedinBC_UL['USGt6000kAndLt10000k_hi_n'] = ""
highestSpeedinBC_UL['USGt10000kAndLt15000k_hi_c'] = ""
highestSpeedinBC_UL['USGt10000kAndLt15000k_hi_s'] = ""
highestSpeedinBC_UL['USGt10000kAndLt15000k_hi_n'] = ""
highestSpeedinBC_UL['USGt15000kAndLt25000k_hi_c'] = ""
highestSpeedinBC_UL['USGt15000kAndLt25000k_hi_s'] = ""
highestSpeedinBC_UL['USGt15000kAndLt25000k_hi_n'] = ""
highestSpeedinBC_UL['USGt25000kAndLt50000k_hi_c'] = ""
highestSpeedinBC_UL['USGt25000kAndLt50000k_hi_s'] = ""
highestSpeedinBC_UL['USGt25000kAndLt50000k_hi_n'] = ""
highestSpeedinBC_UL['USGt50000kAndLt100000k_hi_c'] = ""
highestSpeedinBC_UL['USGt50000kAndLt100000k_hi_s'] = ""
highestSpeedinBC_UL['USGt50000kAndLt100000k_hi_n'] = ""
highestSpeedinBC_UL['USGt100000kAndLt1Gig_hi_c'] = ""
highestSpeedinBC_UL['USGt100000kAndLt1Gig_hi_s'] = ""
highestSpeedinBC_UL['USGt100000kAndLt1Gig_hi_n'] = ""
highestSpeedinBC_UL['USGt1Gig_hi_c'] = ""
highestSpeedinBC_UL['USGt1Gig_hi_s'] = ""
highestSpeedinBC_UL['USGt1Gig_hi_n'] = ""
highestSpeedinBC_UL.US0_hi_c = ConsumerProviders.blockpop_pct_of_county.where(ConsumerProviders.MaxAdUp == 0)
highestSpeedinBC_UL.US0_hi_s = ConsumerProviders.blockpop_pct_of_state.where(ConsumerProviders.MaxAdUp == 0)
highestSpeedinBC_UL.US0_hi_n = ConsumerProviders.blockpop_pct_of_nation.where(ConsumerProviders.MaxAdUp == 0)
highestSpeedinBC_UL.USGt0kAndLt1000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp > 0) & (ConsumerProviders.MaxAdUp < 1))
highestSpeedinBC_UL.USGt0kAndLt1000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp > 0) & (ConsumerProviders.MaxAdUp < 1))
highestSpeedinBC_UL.USGt0kAndLt1000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp > 0) & (ConsumerProviders.MaxAdUp < 1))
highestSpeedinBC_UL.USGt1000kAndLt3000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 1) & (ConsumerProviders.MaxAdUp < 3))
highestSpeedinBC_UL.USGt1000kAndLt3000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 1) & (ConsumerProviders.MaxAdUp < 3))
highestSpeedinBC_UL.USGt1000kAndLt3000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 1) & (ConsumerProviders.MaxAdUp < 3))
highestSpeedinBC_UL.USGt3000kAndLt4000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 3) & (ConsumerProviders.MaxAdUp < 4))
highestSpeedinBC_UL.USGt3000kAndLt4000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 3) & (ConsumerProviders.MaxAdUp < 4))
highestSpeedinBC_UL.USGt3000kAndLt4000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 3) & (ConsumerProviders.MaxAdUp < 4))
highestSpeedinBC_UL.USGt4000kAndLt6000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 4) & (ConsumerProviders.MaxAdUp < 6))
highestSpeedinBC_UL.USGt4000kAndLt6000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 4) & (ConsumerProviders.MaxAdUp < 6))
highestSpeedinBC_UL.USGt4000kAndLt6000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 4) & (ConsumerProviders.MaxAdUp < 6))
highestSpeedinBC_UL.USGt6000kAndLt10000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 6) & (ConsumerProviders.MaxAdUp < 10))
highestSpeedinBC_UL.USGt6000kAndLt10000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 6) & (ConsumerProviders.MaxAdUp < 10))
highestSpeedinBC_UL.USGt6000kAndLt10000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 6) & (ConsumerProviders.MaxAdUp < 10))
highestSpeedinBC_UL.USGt10000kAndLt15000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 10) & (ConsumerProviders.MaxAdUp < 15))
highestSpeedinBC_UL.USGt10000kAndLt15000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 10) & (ConsumerProviders.MaxAdUp < 15))
highestSpeedinBC_UL.USGt10000kAndLt15000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 10) & (ConsumerProviders.MaxAdUp < 15))
highestSpeedinBC_UL.USGt15000kAndLt25000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 15) & (ConsumerProviders.MaxAdUp < 25))
highestSpeedinBC_UL.USGt15000kAndLt25000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 15) & (ConsumerProviders.MaxAdUp < 25))
highestSpeedinBC_UL.USGt15000kAndLt25000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 15) & (ConsumerProviders.MaxAdUp < 25))
highestSpeedinBC_UL.USGt25000kAndLt50000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 25) & (ConsumerProviders.MaxAdUp < 50))
highestSpeedinBC_UL.USGt25000kAndLt50000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 25) & (ConsumerProviders.MaxAdUp < 50))
highestSpeedinBC_UL.USGt25000kAndLt50000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 25) & (ConsumerProviders.MaxAdUp < 50))
highestSpeedinBC_UL.USGt50000kAndLt100000k_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 50) & (ConsumerProviders.MaxAdUp < 100))
highestSpeedinBC_UL.USGt50000kAndLt100000k_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 50) & (ConsumerProviders.MaxAdUp < 100))
highestSpeedinBC_UL.USGt50000kAndLt100000k_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 50) & (ConsumerProviders.MaxAdUp < 100))
highestSpeedinBC_UL.USGt100000kAndLt1Gig_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 100) & (ConsumerProviders.MaxAdUp < 1000))
highestSpeedinBC_UL.USGt100000kAndLt1Gig_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 100) & (ConsumerProviders.MaxAdUp < 1000))
highestSpeedinBC_UL.USGt100000kAndLt1Gig_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 100) & (ConsumerProviders.MaxAdUp < 1000))
highestSpeedinBC_UL.USGt1Gig_hi_c = ConsumerProviders.blockpop_pct_of_county.where((ConsumerProviders.MaxAdUp >= 1000))
highestSpeedinBC_UL.USGt1Gig_hi_s = ConsumerProviders.blockpop_pct_of_state.where((ConsumerProviders.MaxAdUp >= 1000))
highestSpeedinBC_UL.USGt1Gig_hi_n = ConsumerProviders.blockpop_pct_of_nation.where((ConsumerProviders.MaxAdUp >= 1000))

highestSpeedinBC_UL.to_csv(os.path.join('seeULblocks.csv'))

#Table 25 - Sum Upload Speed Tier Percentages of Counties
ULspeed_tiers_hi_c = highestSpeedinBC_UL.groupby(['countyFIPS'])['US0_hi_c','USGt0kAndLt1000k_hi_c','USGt1000kAndLt3000k_hi_c','USGt3000kAndLt4000k_hi_c','USGt4000kAndLt6000k_hi_c','USGt6000kAndLt10000k_hi_c','USGt10000kAndLt15000k_hi_c','USGt15000kAndLt25000k_hi_c','USGt25000kAndLt50000k_hi_c','USGt50000kAndLt100000k_hi_c','USGt100000kAndLt1Gig_hi_c','USGt1Gig_hi_c'].sum()
ULspeed_tiers_hi_c['countyFIPS'] = ULspeed_tiers_hi_c.index
ULspeed_tiers_hi_c['countyFIPS'] = ULspeed_tiers_hi_c['countyFIPS'].astype(int)
ULspeed_tiers_hi_c = ULspeed_tiers_hi_c.rename(columns={'US0_hi_c':'pctUS0_hi_c','USGt0kAndLt1000k_hi_c':'pctUSGt0kAndLt1000k_hi_c','USGt1000kAndLt3000k_hi_c':'pctUSGt1000kAndLt3000k_hi_c','USGt4000kAndLt6000k_hi_c':'pctUSGt4000kAndLt6000k_hi_c','USGt6000kAndLt10000k_hi_c':'pctUSGt6000kAndLt10000k_hi_c','USGt10000kAndLt15000k_hi_c':'pctUSGt10000kAndLt15000k_hi_c','USGt15000kAndLt25000k_hi_c':'pctUSGt15000kAndLt25000k_hi_c','USGt25000kAndLt50000k_hi_c':'pctUSGt25000kAndLt50000k_hi_c','USGt50000kAndLt100000k_hi_c':'pctUSGt50000kAndLt100000k_hi_c','USGt100000kAndLt1Gig_hi_c':'pctUSGt100000kAndLt1Gig_hi_c','USGt1Gig_hi_c':'pctUSGt1Gig_hi_c','USGt3000kAndLt4000k_hi_c':'pctUSGt3000kAndLt4000k_hi_c'})
ULspeed_tiers_hi_c = ULspeed_tiers_hi_c.fillna(0)


#Table 26 - Sum Upload Speed Tier Percentages of States
ULspeed_tiers_hi_s = highestSpeedinBC_UL.groupby(['stateFIPS'])['US0_hi_s','USGt0kAndLt1000k_hi_s','USGt1000kAndLt3000k_hi_s','USGt3000kAndLt4000k_hi_s','USGt4000kAndLt6000k_hi_s','USGt6000kAndLt10000k_hi_s','USGt10000kAndLt15000k_hi_s','USGt15000kAndLt25000k_hi_s','USGt25000kAndLt50000k_hi_s','USGt50000kAndLt100000k_hi_s','USGt100000kAndLt1Gig_hi_s','USGt1Gig_hi_s'].sum()
ULspeed_tiers_hi_s['stateFIPS'] = ULspeed_tiers_hi_s.index
ULspeed_tiers_hi_s['stateFIPS'] = ULspeed_tiers_hi_s['stateFIPS'].astype(int)
ULspeed_tiers_hi_s = ULspeed_tiers_hi_s.rename(columns={'US0_hi_s':'pctUS0_hi_s','USGt0kAndLt1000k_hi_s':'pctUSGt0kAndLt1000k_hi_s','USGt1000kAndLt3000k_hi_s':'pctUSGt1000kAndLt3000k_hi_s','USGt4000kAndLt6000k_hi_s':'pctUSGt4000kAndLt6000k_hi_s','USGt6000kAndLt10000k_hi_s':'pctUSGt6000kAndLt10000k_hi_s','USGt10000kAndLt15000k_hi_s':'pctUSGt10000kAndLt15000k_hi_s','USGt15000kAndLt25000k_hi_s':'pctUSGt15000kAndLt25000k_hi_s','USGt25000kAndLt50000k_hi_s':'pctUSGt25000kAndLt50000k_hi_s','USGt50000kAndLt100000k_hi_s':'pctUSGt50000kAndLt100000k_hi_s','USGt100000kAndLt1Gig_hi_s':'pctUSGt100000kAndLt1Gig_hi_s','USGt1Gig_hi_s':'pctUSGt1Gig_hi_s','USGt3000kAndLt4000k_hi_s':'pctUSGt3000kAndLt4000k_hi_s'})
ULspeed_tiers_hi_s = ULspeed_tiers_hi_s.fillna(0)


#Table 27 - Sum Upload Speed Tier Percentages of Nation
ULspeed_tiers_hi_n = highestSpeedinBC_UL.groupby(['merge_level'])['US0_hi_n','USGt0kAndLt1000k_hi_n','USGt1000kAndLt3000k_hi_n','USGt3000kAndLt4000k_hi_n','USGt4000kAndLt6000k_hi_n','USGt6000kAndLt10000k_hi_n','USGt10000kAndLt15000k_hi_n','USGt15000kAndLt25000k_hi_n','USGt25000kAndLt50000k_hi_n','USGt50000kAndLt100000k_hi_n','USGt100000kAndLt1Gig_hi_n','USGt1Gig_hi_n'].sum()
ULspeed_tiers_hi_n = ULspeed_tiers_hi_n.rename(columns={'US0_hi_n':'pctUS0_hi_n','USGt0kAndLt1000k_hi_n':'pctUSGt0kAndLt1000k_hi_n','USGt1000kAndLt3000k_hi_n':'pctUSGt1000kAndLt3000k_hi_n','USGt4000kAndLt6000k_hi_n':'pctUSGt4000kAndLt6000k_hi_n','USGt6000kAndLt10000k_hi_n':'pctUSGt6000kAndLt10000k_hi_n','USGt10000kAndLt15000k_hi_n':'pctUSGt10000kAndLt15000k_hi_n','USGt15000kAndLt25000k_hi_n':'pctUSGt15000kAndLt25000k_hi_n','USGt25000kAndLt50000k_hi_n':'pctUSGt25000kAndLt50000k_hi_n','USGt50000kAndLt100000k_hi_n':'pctUSGt50000kAndLt100000k_hi_n','USGt100000kAndLt1Gig_hi_n':'pctUSGt100000kAndLt1Gig_hi_n','USGt1Gig_hi_n':'pctUSGt1Gig_hi_n','USGt3000kAndLt4000k_hi_n':'pctUSGt3000kAndLt4000k_hi_n'})
ULspeed_tiers_hi_n = ULspeed_tiers_hi_n.drop_duplicates()
ULspeed_tiers_hi_n['merge_level'] = 'national'

ULspeed_tiers_hi_c['countyFIPS'] = ULspeed_tiers_hi_c['countyFIPS'].apply('{:0>5}'.format).astype(int)
ULspeed_tiers_hi_s['stateFIPS'] = ULspeed_tiers_hi_s['stateFIPS'].apply('{:0>2}'.format).astype(int)


ULspeed_tiers_hi_s.to_csv(os.path.join(workdir, r'ULspeed_tiers_hi_s.csv'), encoding='utf-8')
ULspeed_tiers_hi_c.to_csv(os.path.join(workdir, r'ULspeed_tiers_hi_c.csv'), encoding='utf-8')
ULspeed_tiers_hi_n.to_csv(os.path.join(workdir, r'ULspeed_tiers_hi_n.csv'), encoding='utf-8')

#Join County Data with County and State Download Speed Tiers
county_data = county_data.merge(ULspeed_tiers_hi_c,how='inner',on='countyFIPS')
county_data = county_data.merge(ULspeed_tiers_hi_s,how='inner',on='stateFIPS')
county_data = county_data.merge(ULspeed_tiers_hi_n,how='inner',on='merge_level')
county_data = county_data.drop_duplicates()
county_data_mcspeeds = county_data.copy()

## WRITE OUT TABLE FOR NEXT STEP ##
county_data_mcspeeds.to_csv(os.path.join(workdir, r'county_data_mcspeeds.csv'), encoding='utf-8')

import pandas as pd
import signal
import os
import numpy
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'

county_data_mcspeeds = pd.read_csv(r'county_data_mcspeeds.csv')

county_data_mcspeeds['mcds_prop_c'] = county_data_mcspeeds[['pctDS0_hi_c','pctDSGt0kAndLt1000k_hi_c','pctDSGt1000kAndLt3000k_hi_c','pctDSGt3000kAndLt4000k_hi_c','pctDSGt4000kAndLt6000k_hi_c','pctDSGt6000kAndLt10000k_hi_c','pctDSGt10000kAndLt15000k_hi_c','pctDSGt15000kAndLt25000k_hi_c','pctDSGt25000kAndLt50000k_hi_c','pctDSGt50000kAndLt100000k_hi_c','pctDSGt100000kAndLt1Gig_hi_c','pctDSGt1Gig_hi_c']].max(axis=1)
county_data_mcspeeds['mcds_tier_c'] = county_data_mcspeeds[['pctDS0_hi_c','pctDSGt0kAndLt1000k_hi_c','pctDSGt1000kAndLt3000k_hi_c','pctDSGt3000kAndLt4000k_hi_c','pctDSGt4000kAndLt6000k_hi_c','pctDSGt6000kAndLt10000k_hi_c','pctDSGt10000kAndLt15000k_hi_c','pctDSGt15000kAndLt25000k_hi_c','pctDSGt25000kAndLt50000k_hi_c','pctDSGt50000kAndLt100000k_hi_c','pctDSGt100000kAndLt1Gig_hi_c','pctDSGt1Gig_hi_c']].idxmax(axis=1)

county_data_mcspeeds['mcds_prop_s'] = county_data_mcspeeds[['pctDS0_hi_s','pctDSGt0kAndLt1000k_hi_s','pctDSGt1000kAndLt3000k_hi_s','pctDSGt3000kAndLt4000k_hi_s','pctDSGt4000kAndLt6000k_hi_s','pctDSGt6000kAndLt10000k_hi_s','pctDSGt10000kAndLt15000k_hi_s','pctDSGt15000kAndLt25000k_hi_s','pctDSGt25000kAndLt50000k_hi_s','pctDSGt50000kAndLt100000k_hi_s','pctDSGt100000kAndLt1Gig_hi_s','pctDSGt1Gig_hi_s']].max(axis=1)
county_data_mcspeeds['mcds_tier_s'] = county_data_mcspeeds[['pctDS0_hi_s','pctDSGt0kAndLt1000k_hi_s','pctDSGt1000kAndLt3000k_hi_s','pctDSGt3000kAndLt4000k_hi_s','pctDSGt4000kAndLt6000k_hi_s','pctDSGt6000kAndLt10000k_hi_s','pctDSGt10000kAndLt15000k_hi_s','pctDSGt15000kAndLt25000k_hi_s','pctDSGt25000kAndLt50000k_hi_s','pctDSGt50000kAndLt100000k_hi_s','pctDSGt100000kAndLt1Gig_hi_s','pctDSGt1Gig_hi_s']].idxmax(axis=1)

county_data_mcspeeds['mcds_prop_n'] = county_data_mcspeeds[['pctDS0_hi_n','pctDSGt0kAndLt1000k_hi_n','pctDSGt1000kAndLt3000k_hi_n','pctDSGt3000kAndLt4000k_hi_n','pctDSGt4000kAndLt6000k_hi_n','pctDSGt6000kAndLt10000k_hi_n','pctDSGt10000kAndLt15000k_hi_n','pctDSGt15000kAndLt25000k_hi_n','pctDSGt25000kAndLt50000k_hi_n','pctDSGt50000kAndLt100000k_hi_n','pctDSGt100000kAndLt1Gig_hi_n','pctDSGt1Gig_hi_n']].max(axis=1)
county_data_mcspeeds['mcds_tier_n'] = county_data_mcspeeds[['pctDS0_hi_n','pctDSGt0kAndLt1000k_hi_s','pctDSGt1000kAndLt3000k_hi_n','pctDSGt3000kAndLt4000k_hi_n','pctDSGt4000kAndLt6000k_hi_n','pctDSGt6000kAndLt10000k_hi_n','pctDSGt10000kAndLt15000k_hi_n','pctDSGt15000kAndLt25000k_hi_n','pctDSGt25000kAndLt50000k_hi_n','pctDSGt50000kAndLt100000k_hi_n','pctDSGt100000kAndLt1Gig_hi_n','pctDSGt1Gig_hi_n']].idxmax(axis=1)

county_data_mcspeeds['mcus_prop_c'] = county_data_mcspeeds[['pctUS0_hi_c','pctUSGt0kAndLt1000k_hi_c','pctUSGt1000kAndLt3000k_hi_c','pctUSGt3000kAndLt4000k_hi_c','pctUSGt4000kAndLt6000k_hi_c','pctUSGt6000kAndLt10000k_hi_c','pctUSGt10000kAndLt15000k_hi_c','pctUSGt15000kAndLt25000k_hi_c','pctUSGt25000kAndLt50000k_hi_c','pctUSGt50000kAndLt100000k_hi_c','pctUSGt100000kAndLt1Gig_hi_c','pctUSGt1Gig_hi_c']].max(axis=1)
county_data_mcspeeds['mcus_tier_c'] = county_data_mcspeeds[['pctUS0_hi_c','pctUSGt0kAndLt1000k_hi_c','pctUSGt1000kAndLt3000k_hi_c','pctUSGt3000kAndLt4000k_hi_c','pctUSGt4000kAndLt6000k_hi_c','pctUSGt6000kAndLt10000k_hi_c','pctUSGt10000kAndLt15000k_hi_c','pctUSGt15000kAndLt25000k_hi_c','pctUSGt25000kAndLt50000k_hi_c','pctUSGt50000kAndLt100000k_hi_c','pctUSGt100000kAndLt1Gig_hi_c','pctUSGt1Gig_hi_c']].idxmax(axis=1)

county_data_mcspeeds['mcus_prop_s'] = county_data_mcspeeds[['pctUS0_hi_s','pctUSGt0kAndLt1000k_hi_s','pctUSGt1000kAndLt3000k_hi_s','pctUSGt3000kAndLt4000k_hi_s','pctUSGt4000kAndLt6000k_hi_s','pctUSGt6000kAndLt10000k_hi_s','pctUSGt10000kAndLt15000k_hi_s','pctUSGt15000kAndLt25000k_hi_s','pctUSGt25000kAndLt50000k_hi_s','pctUSGt50000kAndLt100000k_hi_s','pctUSGt100000kAndLt1Gig_hi_s','pctUSGt1Gig_hi_s']].max(axis=1)
county_data_mcspeeds['mcus_tier_s'] = county_data_mcspeeds[['pctUS0_hi_s','pctUSGt0kAndLt1000k_hi_s','pctUSGt1000kAndLt3000k_hi_s','pctUSGt3000kAndLt4000k_hi_s','pctUSGt4000kAndLt6000k_hi_s','pctUSGt6000kAndLt10000k_hi_s','pctUSGt10000kAndLt15000k_hi_s','pctUSGt15000kAndLt25000k_hi_s','pctUSGt25000kAndLt50000k_hi_s','pctUSGt50000kAndLt100000k_hi_s','pctUSGt100000kAndLt1Gig_hi_s','pctUSGt1Gig_hi_s']].idxmax(axis=1)

county_data_mcspeeds['mcus_prop_n'] = county_data_mcspeeds[['pctUS0_hi_n','pctUSGt0kAndLt1000k_hi_n','pctUSGt1000kAndLt3000k_hi_n','pctUSGt3000kAndLt4000k_hi_n','pctUSGt4000kAndLt6000k_hi_n','pctUSGt6000kAndLt10000k_hi_n','pctUSGt10000kAndLt15000k_hi_n','pctUSGt15000kAndLt25000k_hi_n','pctUSGt25000kAndLt50000k_hi_n','pctUSGt50000kAndLt100000k_hi_n','pctUSGt100000kAndLt1Gig_hi_n','pctUSGt1Gig_hi_n']].max(axis=1)
county_data_mcspeeds['mcus_tier_n'] = county_data_mcspeeds[['pctUS0_hi_n','pctUSGt0kAndLt1000k_hi_s','pctUSGt1000kAndLt3000k_hi_n','pctUSGt3000kAndLt4000k_hi_n','pctUSGt4000kAndLt6000k_hi_n','pctUSGt6000kAndLt10000k_hi_n','pctUSGt10000kAndLt15000k_hi_n','pctUSGt15000kAndLt25000k_hi_n','pctUSGt25000kAndLt50000k_hi_n','pctUSGt50000kAndLt100000k_hi_n','pctUSGt100000kAndLt1Gig_hi_n','pctUSGt1Gig_hi_n']].idxmax(axis=1)

county_data_mcspeeds['totalDSPCT'] = county_data_mcspeeds['pctDS0_hi_c']+county_data_mcspeeds['pctDSGt0kAndLt1000k_hi_c']+county_data_mcspeeds['pctDSGt1000kAndLt3000k_hi_c']+county_data_mcspeeds['pctDSGt3000kAndLt4000k_hi_c']+county_data_mcspeeds['pctDSGt4000kAndLt6000k_hi_c']+county_data_mcspeeds['pctDSGt6000kAndLt10000k_hi_c']+county_data_mcspeeds['pctDSGt10000kAndLt15000k_hi_c']+county_data_mcspeeds['pctDSGt15000kAndLt25000k_hi_c']+county_data_mcspeeds['pctDSGt25000kAndLt50000k_hi_c']+county_data_mcspeeds['pctDSGt50000kAndLt100000k_hi_c']+county_data_mcspeeds['pctDSGt100000kAndLt1Gig_hi_c']+county_data_mcspeeds['pctDSGt1Gig_hi_c']
county_data_mcspeeds['totalUSPCT'] = county_data_mcspeeds['pctUS0_hi_c']+county_data_mcspeeds['pctUSGt0kAndLt1000k_hi_c']+county_data_mcspeeds['pctUSGt1000kAndLt3000k_hi_c']+county_data_mcspeeds['pctUSGt3000kAndLt4000k_hi_c']+county_data_mcspeeds['pctUSGt4000kAndLt6000k_hi_c']+county_data_mcspeeds['pctUSGt6000kAndLt10000k_hi_c']+county_data_mcspeeds['pctUSGt10000kAndLt15000k_hi_c']+county_data_mcspeeds['pctUSGt15000kAndLt25000k_hi_c']+county_data_mcspeeds['pctUSGt25000kAndLt50000k_hi_c']+county_data_mcspeeds['pctUSGt50000kAndLt100000k_hi_c']+county_data_mcspeeds['pctUSGt100000kAndLt1Gig_hi_c']+county_data_mcspeeds['pctUSGt1Gig_hi_c']


## WRITE OUT TABLE FOR NEXT STEP ##
county_data_mcspeeds.to_csv(os.path.join(workdir, r'county_data_mcspeeds.csv'), encoding='utf-8')

import pandas as pd
import signal
import os
import sys
import numpy
import functools as func
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'

natlbb_pop = pd.read_csv(r'NatlbbWithPopulation.csv', usecols = ['Consumer','Business','BlockCode','HocoNum','countyFIPS','stateFIPS'])


#Tables 29:31 - Create Tables of Block Code Provider Count
freq_prov_c = natlbb_pop.query("Consumer == 1").groupby(['BlockCode']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'prov_cons'})
freq_prov_b = natlbb_pop.query("Business == 1").groupby(['BlockCode']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'prov_bus'})
freq_prov_a = natlbb_pop.query("Business == 1 or Consumer == 1").groupby(['BlockCode']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'prov_all'})

#Tables 31:34 - Create Tables of County Provider Count
provcount_cnty_c = natlbb_pop.query("Consumer == 1").groupby(['countyFIPS']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'provcount_cnty_c'})
provcount_cnty_b = natlbb_pop.query("Business == 1").groupby(['countyFIPS']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'provcount_cnty_b'})
provcount_cnty_a = natlbb_pop.query("Business == 1 or Consumer == 1").groupby(['countyFIPS']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'provcount_cnty_a'})

#Table 35 - Join County Number of Providers by Type and County on County FIPS
provcounts = [provcount_cnty_a,provcount_cnty_b,provcount_cnty_c]
provcount_cnty_total = func.reduce(lambda left,right: pd.merge(left,right,on='countyFIPS'), provcounts)

#Tables 36:39 - Create Tables of State Provider Count
provcount_state_c = natlbb_pop.query("Consumer == 1").groupby(['stateFIPS']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'provcount_state_c'})
provcount_state_b = natlbb_pop.query("Business == 1").groupby(['stateFIPS']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'provcount_state_b'})
provcount_state_a = natlbb_pop.query("Business == 1 or Consumer == 1").groupby(['stateFIPS']).HocoNum.nunique().reset_index().rename(columns = {'HocoNum':'provcount_state_a'})

#Table 40 - Join County Number of Providers by Type and County on County FIPS
statecount = [provcount_state_c,provcount_state_b,provcount_state_a]
provcount_state_total = func.reduce(lambda left,right: pd.merge(left,right,on='stateFIPS'), statecount)

#Tables 41:43 - Create Tables of National Provider Count
provcount_nat_c_count = natlbb_pop.query("Consumer == 1").HocoNum.nunique()
provcount_nat_c = pd.DataFrame(columns=['provcount_nat_c'])
provcount_nat_c.loc[1] = provcount_nat_c_count
provcount_nat_c['merge_level'] = 'national'

provcount_nat_b_count = natlbb_pop.query("Business == 1").HocoNum.nunique()
provcount_nat_b = pd.DataFrame(columns=['provcount_nat_b'])
provcount_nat_b.loc[1] = provcount_nat_b_count
provcount_nat_b['merge_level'] = 'national'

provcount_nat_a_count = natlbb_pop.query("Business == 1 or Consumer == 1").HocoNum.nunique()
provcount_nat_a = pd.DataFrame(columns=['provcount_nat_a'])
provcount_nat_a.loc[1] = provcount_nat_a_count
provcount_nat_a['merge_level'] = 'national'

natcount = [provcount_nat_c,provcount_nat_b,provcount_nat_a]
provcount_nat_total = func.reduce(lambda left,right: pd.merge(left,right,on='merge_level'), natcount)

#provcount_cnty_total['provcount_cnty_a'] = provcount_cnty_total['provcount_cnty_b'] + provcount_cnty_total['provcount_cnty_c']
#provcount_state_total['provcount_state_a'] = provcount_state_total['provcount_state_b'] + provcount_state_total['provcount_state_c']
#provcount_nat_total['provcount_nat_a'] = provcount_nat_total['provcount_nat_b'] + provcount_nat_total['provcount_nat_c']

## WRITE OUT TABLE FOR NEXT STEP ##

provcount_nat_total.to_csv(os.path.join(workdir, r'provcount_nat_total.csv'), encoding='utf-8')
provcount_cnty_total.to_csv(os.path.join(workdir, r'provcount_cnty_total.csv'), encoding='utf-8')
provcount_state_total.to_csv(os.path.join(workdir, r'provcount_state_total.csv'), encoding='utf-8')
freq_prov_c.to_csv(os.path.join(workdir, r'freq_prov_c.csv'), encoding='utf-8')
freq_prov_b.to_csv(os.path.join(workdir, r'freq_prov_b.csv'), encoding='utf-8')
freq_prov_a.to_csv(os.path.join(workdir, r'freq_prov_a.csv'), encoding='utf-8')

import pandas as pd
import signal
import os
import numpy
import functools as func
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'

freq_prov_b = pd.read_csv(r'freq_prov_b.csv')
freq_prov_c = pd.read_csv(r'freq_prov_c.csv')
freq_prov_a = pd.read_csv(r'freq_prov_a.csv')
freq_prov_a['prov_count_all'] = freq_prov_c['prov_cons'] + freq_prov_b['prov_bus']
provcount_cnty_total = pd.read_csv(r'provcount_cnty_total.csv')
provcount_state_total = pd.read_csv(r'provcount_state_total.csv')
provcount_nat_total = pd.read_csv(r'provcount_nat_total.csv')
bb_avail_all_pop = pd.read_csv(r'ResultConsumerProviders.csv')
county_data = pd.read_csv(r'county_data_mcspeeds.csv')

#Table 45 - Merge Unique Provider Type Frequency Tables by Block Type into One Table
freq_tables = [freq_prov_c,freq_prov_b,freq_prov_a]
freq_prov_all = func.reduce(lambda left,right: pd.merge(left,right,on='BlockCode'), freq_tables)

#Table 46 - Merge Provider Frequency Table with Broadband Population Tables
num_provs = freq_prov_all.merge(bb_avail_all_pop, on='BlockCode')

#Merge County, State and National Provider Counts with County Data
county_data = county_data.merge(provcount_cnty_total, on='countyFIPS')
county_data = county_data.merge(provcount_state_total, on='stateFIPS')
county_data['merge_level'] = 'national'
county_data = county_data.merge(provcount_nat_total, on='merge_level')
county_data_final_hi_common = county_data.copy()


## WRITE OUT TABLES FOR NEXT STEP ##

county_data_final_hi_common.to_csv(os.path.join(workdir, r'county_data_final_hi_common.csv'), encoding='utf-8')
num_provs.to_csv(os.path.join(workdir, r'num_provs.csv'), encoding='utf-8')

import pandas as pd
import signal
import os
import numpy
import functools as func
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'

num_provs = pd.read_csv(r'num_provs.csv')
num_provs = num_provs[['BlockCode','prov_cons','prov_bus','prov_all','countyFIPS','stateFIPS','blockpop_pct_of_county','blockpop_pct_of_state','blockpop_pct_of_nation','gl_pop','CountyPop']]
num_provs = num_provs.drop_duplicates(inplace=False)

num_provs['prov_cnty_c_0'] = num_provs.query('prov_cons == 0')['blockpop_pct_of_county']
num_provs['prov_state_c_0'] = num_provs.query('prov_cons == 0')['blockpop_pct_of_state']
num_provs['prov_nat_c_0'] = num_provs.query('prov_cons == 0')['blockpop_pct_of_nation']

num_provs['prov_cnty_c_1'] = num_provs.query('prov_cons == 1')['blockpop_pct_of_county']
num_provs['prov_state_c_1'] = num_provs.query('prov_cons == 1')['blockpop_pct_of_state']
num_provs['prov_nat_c_1'] = num_provs.query('prov_cons == 1')['blockpop_pct_of_nation']

num_provs['prov_cnty_c_2'] = num_provs.query('prov_cons == 2')['blockpop_pct_of_county']
num_provs['prov_state_c_2'] = num_provs.query('prov_cons == 2')['blockpop_pct_of_state']
num_provs['prov_nat_c_2'] = num_provs.query('prov_cons == 2')['blockpop_pct_of_nation']

num_provs['prov_cnty_c_3'] = num_provs.query('prov_cons == 3')['blockpop_pct_of_county']
num_provs['prov_state_c_3'] = num_provs.query('prov_cons == 3')['blockpop_pct_of_state']
num_provs['prov_nat_c_3'] = num_provs.query('prov_cons == 3')['blockpop_pct_of_nation']

num_provs['prov_cnty_c_4'] = num_provs.query('prov_cons == 4')['blockpop_pct_of_county']
num_provs['prov_state_c_4'] = num_provs.query('prov_cons == 4')['blockpop_pct_of_state']
num_provs['prov_nat_c_4'] = num_provs.query('prov_cons == 4')['blockpop_pct_of_nation']

num_provs['prov_cnty_c_5'] = num_provs.query('prov_cons == 5')['blockpop_pct_of_county']
num_provs['prov_state_c_5'] = num_provs.query('prov_cons == 5')['blockpop_pct_of_state']
num_provs['prov_nat_c_5'] = num_provs.query('prov_cons == 5')['blockpop_pct_of_nation']

num_provs['prov_cnty_c_6'] = num_provs.query('prov_cons == 6')['blockpop_pct_of_county']
num_provs['prov_state_c_6'] = num_provs.query('prov_cons == 6')['blockpop_pct_of_state']
num_provs['prov_nat_c_6'] = num_provs.query('prov_cons == 6')['blockpop_pct_of_nation']

num_provs['prov_cnty_c_7'] = num_provs.query('prov_cons == 7')['blockpop_pct_of_county']
num_provs['prov_state_c_7'] = num_provs.query('prov_cons == 7')['blockpop_pct_of_state']
num_provs['prov_nat_c_7'] = num_provs.query('prov_cons == 7')['blockpop_pct_of_nation']

num_provs['prov_cnty_greq_c_8'] = num_provs.query('prov_cons == 8')['blockpop_pct_of_county']
num_provs['prov_state_greq_c_8'] = num_provs.query('prov_cons == 8')['blockpop_pct_of_state']
num_provs['prov_nat_greq_c_8'] = num_provs.query('prov_cons == 8')['blockpop_pct_of_nation']

num_provs['prov_cnty_b_0'] = num_provs.query('prov_bus == 0')['blockpop_pct_of_county']
num_provs['prov_state_b_0'] = num_provs.query('prov_bus == 0')['blockpop_pct_of_state']
num_provs['prov_nat_b_0'] = num_provs.query('prov_bus == 0')['blockpop_pct_of_nation']

num_provs['prov_cnty_b_1'] = num_provs.query('prov_bus == 1')['blockpop_pct_of_county']
num_provs['prov_state_b_1'] = num_provs.query('prov_bus == 1')['blockpop_pct_of_state']
num_provs['prov_nat_b_1'] = num_provs.query('prov_bus == 1')['blockpop_pct_of_nation']

num_provs['prov_cnty_b_2'] = num_provs.query('prov_bus == 2')['blockpop_pct_of_county']
num_provs['prov_state_b_2'] = num_provs.query('prov_bus == 2')['blockpop_pct_of_state']
num_provs['prov_nat_b_2'] = num_provs.query('prov_bus == 2')['blockpop_pct_of_nation']

num_provs['prov_cnty_b_3'] = num_provs.query('prov_bus == 3')['blockpop_pct_of_county']
num_provs['prov_state_b_3'] = num_provs.query('prov_bus == 3')['blockpop_pct_of_state']
num_provs['prov_nat_b_3'] = num_provs.query('prov_bus == 3')['blockpop_pct_of_nation']

num_provs['prov_cnty_b_4'] = num_provs.query('prov_bus == 4')['blockpop_pct_of_county']
num_provs['prov_state_b_4'] = num_provs.query('prov_bus == 4')['blockpop_pct_of_state']
num_provs['prov_nat_b_4'] = num_provs.query('prov_bus == 4')['blockpop_pct_of_nation']

num_provs['prov_cnty_b_5'] = num_provs.query('prov_bus == 5')['blockpop_pct_of_county']
num_provs['prov_state_b_5'] = num_provs.query('prov_bus == 5')['blockpop_pct_of_state']
num_provs['prov_nat_b_5'] = num_provs.query('prov_bus == 5')['blockpop_pct_of_nation']

num_provs['prov_cnty_b_6'] = num_provs.query('prov_bus == 6')['blockpop_pct_of_county']
num_provs['prov_state_b_6'] = num_provs.query('prov_bus == 6')['blockpop_pct_of_state']
num_provs['prov_nat_b_6'] = num_provs.query('prov_bus == 6')['blockpop_pct_of_nation']

num_provs['prov_cnty_b_7'] = num_provs.query('prov_bus == 7')['blockpop_pct_of_county']
num_provs['prov_state_b_7'] = num_provs.query('prov_bus == 7')['blockpop_pct_of_state']
num_provs['prov_nat_b_7'] = num_provs.query('prov_bus == 7')['blockpop_pct_of_nation']

num_provs['prov_cnty_greq_b_8'] = num_provs.query('prov_bus == 8')['blockpop_pct_of_county']
num_provs['prov_state_greq_b_8'] = num_provs.query('prov_bus == 8')['blockpop_pct_of_state']
num_provs['prov_nat_greq_b_8'] = num_provs.query('prov_bus == 8')['blockpop_pct_of_nation']

num_provs['prov_cnty_a_0'] = num_provs.query('prov_all == 0')['blockpop_pct_of_county']
num_provs['prov_state_a_0'] = num_provs.query('prov_all == 0')['blockpop_pct_of_state']
num_provs['prov_nat_a_0'] = num_provs.query('prov_all == 0')['blockpop_pct_of_nation']

num_provs['prov_cnty_a_1'] = num_provs.query('prov_all == 1')['blockpop_pct_of_county']
num_provs['prov_state_a_1'] = num_provs.query('prov_all == 1')['blockpop_pct_of_state']
num_provs['prov_nat_a_1'] = num_provs.query('prov_all == 1')['blockpop_pct_of_nation']

num_provs['prov_cnty_a_2'] = num_provs.query('prov_all == 2')['blockpop_pct_of_county']
num_provs['prov_state_a_2'] = num_provs.query('prov_all == 2')['blockpop_pct_of_state']
num_provs['prov_nat_a_2'] = num_provs.query('prov_all == 2')['blockpop_pct_of_nation']

num_provs['prov_cnty_a_3'] = num_provs.query('prov_all == 3')['blockpop_pct_of_county']
num_provs['prov_state_a_3'] = num_provs.query('prov_all == 3')['blockpop_pct_of_state']
num_provs['prov_nat_a_3'] = num_provs.query('prov_all == 3')['blockpop_pct_of_nation']

num_provs['prov_cnty_a_4'] = num_provs.query('prov_all == 4')['blockpop_pct_of_county']
num_provs['prov_state_a_4'] = num_provs.query('prov_all == 4')['blockpop_pct_of_state']
num_provs['prov_nat_a_4'] = num_provs.query('prov_all == 4')['blockpop_pct_of_nation']

num_provs['prov_cnty_a_5'] = num_provs.query('prov_all == 5')['blockpop_pct_of_county']
num_provs['prov_state_a_5'] = num_provs.query('prov_all == 5')['blockpop_pct_of_state']
num_provs['prov_nat_a_5'] = num_provs.query('prov_all == 5')['blockpop_pct_of_nation']

num_provs['prov_cnty_a_6'] = num_provs.query('prov_all == 6')['blockpop_pct_of_county']
num_provs['prov_state_a_6'] = num_provs.query('prov_all == 6')['blockpop_pct_of_state']
num_provs['prov_nat_a_6'] = num_provs.query('prov_all == 6')['blockpop_pct_of_nation']

num_provs['prov_cnty_a_7'] = num_provs.query('prov_all == 7')['blockpop_pct_of_county']
num_provs['prov_state_a_7'] = num_provs.query('prov_all == 7')['blockpop_pct_of_state']
num_provs['prov_nat_a_7'] = num_provs.query('prov_all == 7')['blockpop_pct_of_nation']

num_provs['prov_cnty_greq_a_8'] = num_provs.query('prov_all == 8')['blockpop_pct_of_county']
num_provs['prov_state_greq_a_8'] = num_provs.query('prov_all == 8')['blockpop_pct_of_state']
num_provs['prov_nat_greq_a_8'] = num_provs.query('prov_all == 8')['blockpop_pct_of_nation']


## WRITE OUT TABLES FOR NEXT STEP ##

num_provs.to_csv(os.path.join(workdir, r'num_provs.csv'), encoding='utf-8')

import pandas as pd
import signal
import os
import numpy
import functools as func
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'

num_provs = pd.read_csv(r'num_provs.csv')
num_provs['merge_level'] = 'national'

num_provs_a_county = num_provs.groupby(['countyFIPS'])["prov_cnty_a_0","prov_cnty_a_1","prov_cnty_a_2","prov_cnty_a_3","prov_cnty_a_4","prov_cnty_a_5","prov_cnty_a_6","prov_cnty_a_7","prov_cnty_greq_a_8"].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_cnty_a_0':'sum_prov_cnty_a_0','prov_cnty_a_1':'sum_prov_cnty_a_1','prov_cnty_a_2':'sum_prov_cnty_a_2','prov_cnty_a_3':'sum_prov_cnty_a_3','prov_cnty_a_4':'sum_prov_cnty_a_4','prov_cnty_a_5':'sum_prov_cnty_a_5','prov_cnty_a_6':'sum_prov_cnty_a_6','prov_cnty_a_7':'sum_prov_cnty_a_7','prov_cnty_greq_a_8':'sum_prov_cnty_greq_a_8'})
num_provs_a_county = num_provs_a_county.reset_index()
num_provs_a_county_join_fields = num_provs[['stateFIPS','CountyPop','countyFIPS']].drop_duplicates()
num_provs_a_county = num_provs_a_county.merge(num_provs_a_county_join_fields, how='inner', on='countyFIPS')
num_provs_a_county.to_csv(os.path.join(workdir, r'num_provs_a_county.csv'), encoding='utf-8')

state_pop = num_provs_a_county.groupby(['stateFIPS'], as_index = False)['CountyPop'].sum()
state_pop = state_pop.rename(columns={'CountyPop': 'StatePop'})
num_provs_a_state = num_provs.groupby(['stateFIPS'])["prov_state_a_0","prov_state_a_1","prov_state_a_2","prov_state_a_3","prov_state_a_4","prov_state_a_5","prov_state_a_6","prov_state_a_7","prov_state_greq_a_8"].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_state_a_0':'sum_prov_state_a_0','prov_state_a_1':'sum_prov_state_a_1','prov_state_a_2':'sum_prov_state_a_2','prov_state_a_3':'sum_prov_state_a_3','prov_state_a_4':'sum_prov_state_a_4','prov_state_a_5':'sum_prov_state_a_5','prov_state_a_6':'sum_prov_state_a_6','prov_state_a_7':'sum_prov_state_a_7','prov_state_greq_a_8':'sum_prov_state_greq_a_8'})
num_provs_a_state = num_provs_a_state.reset_index()
num_provs_a_state = num_provs_a_state.merge(state_pop, how='inner', on='stateFIPS')
num_provs_a_state.to_csv(os.path.join(workdir, r'num_provs_a_state.csv'), encoding='utf-8')

num_provs_a_nat = num_provs.groupby(['merge_level'], as_index = False)['prov_nat_a_0','prov_nat_a_1','prov_nat_a_2','prov_nat_a_3','prov_nat_a_4','prov_nat_a_5','prov_nat_a_6','prov_nat_a_7','prov_nat_greq_a_8'].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_nat_a_0':'sum_prov_nat_a_0','prov_nat_a_1':'sum_prov_nat_a_1','prov_nat_a_2':'sum_prov_nat_a_2','prov_nat_a_3':'sum_prov_nat_a_3','prov_nat_a_4':'sum_prov_nat_a_4','prov_nat_a_5':'sum_prov_nat_a_5','prov_nat_a_6':'sum_prov_nat_a_6','prov_nat_a_7':'sum_prov_nat_a_7','prov_nat_greq_a_8':'sum_prov_nat_greq_a_8'})
num_provs_a_nat['merge_level'] = 'national'
num_provs_a_nat.to_csv(os.path.join(workdir, r'num_provs_a_nat.csv'), encoding='utf-8')


num_provs_b_county = num_provs.groupby(['countyFIPS'])["prov_cnty_b_0","prov_cnty_b_1","prov_cnty_b_2","prov_cnty_b_3","prov_cnty_b_4","prov_cnty_b_5","prov_cnty_b_6","prov_cnty_b_7","prov_cnty_greq_b_8"].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_cnty_b_0':'sum_prov_cnty_b_0','prov_cnty_b_1':'sum_prov_cnty_b_1','prov_cnty_b_2':'sum_prov_cnty_b_2','prov_cnty_b_3':'sum_prov_cnty_b_3','prov_cnty_b_4':'sum_prov_cnty_b_4','prov_cnty_b_5':'sum_prov_cnty_b_5','prov_cnty_b_6':'sum_prov_cnty_b_6','prov_cnty_b_7':'sum_prov_cnty_b_7','prov_cnty_greq_b_8':'sum_prov_cnty_greq_b_8'})
num_provs_b_county = num_provs_b_county.reset_index()
num_provs_b_county_join_fields = num_provs[['stateFIPS','CountyPop','countyFIPS']].drop_duplicates()
num_provs_b_county = num_provs_b_county.merge(num_provs_b_county_join_fields, how='inner', on='countyFIPS')
num_provs_b_county.to_csv(os.path.join(workdir, r'num_provs_b_county.csv'), encoding='utf-8')

state_pop = num_provs_b_county.groupby(['stateFIPS'], as_index = False)['CountyPop'].sum()
state_pop = state_pop.rename(columns={'CountyPop': 'StatePop'})
num_provs_b_state = num_provs.groupby(['stateFIPS'])["prov_state_b_0","prov_state_b_1","prov_state_b_2","prov_state_b_3","prov_state_b_4","prov_state_b_5","prov_state_b_6","prov_state_b_7","prov_state_greq_b_8"].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_state_b_0':'sum_prov_state_b_0','prov_state_b_1':'sum_prov_state_b_1','prov_state_b_2':'sum_prov_state_b_2','prov_state_b_3':'sum_prov_state_b_3','prov_state_b_4':'sum_prov_state_b_4','prov_state_b_5':'sum_prov_state_b_5','prov_state_b_6':'sum_prov_state_b_6','prov_state_b_7':'sum_prov_state_b_7','prov_state_greq_b_8':'sum_prov_state_greq_b_8'})
num_provs_b_state = num_provs_b_state.reset_index()
num_provs_b_state = num_provs_b_state.merge(state_pop, how='inner', on='stateFIPS')
num_provs_b_state.to_csv(os.path.join(workdir, r'num_provs_b_state.csv'), encoding='utf-8')

num_provs_b_nat = num_provs.groupby(['merge_level'], as_index = False)['prov_nat_b_0','prov_nat_b_1','prov_nat_b_2','prov_nat_b_3','prov_nat_b_4','prov_nat_b_5','prov_nat_b_6','prov_nat_b_7','prov_nat_greq_b_8'].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_nat_b_0':'sum_prov_nat_b_0','prov_nat_b_1':'sum_prov_nat_b_1','prov_nat_b_2':'sum_prov_nat_b_2','prov_nat_b_3':'sum_prov_nat_b_3','prov_nat_b_4':'sum_prov_nat_b_4','prov_nat_b_5':'sum_prov_nat_b_5','prov_nat_b_6':'sum_prov_nat_b_6','prov_nat_b_7':'sum_prov_nat_b_7','prov_nat_greq_b_8':'sum_prov_nat_greq_b_8'})
num_provs_b_nat['merge_level'] = 'national'
num_provs_b_nat.to_csv(os.path.join(workdir, r'num_provs_b_nat.csv'), encoding='utf-8')


num_provs_c_county = num_provs.groupby(['countyFIPS'])["prov_cnty_c_0","prov_cnty_c_1","prov_cnty_c_2","prov_cnty_c_3","prov_cnty_c_4","prov_cnty_c_5","prov_cnty_c_6","prov_cnty_c_7","prov_cnty_greq_c_8"].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_cnty_c_0':'sum_prov_cnty_c_0','prov_cnty_c_1':'sum_prov_cnty_c_1','prov_cnty_c_2':'sum_prov_cnty_c_2','prov_cnty_c_3':'sum_prov_cnty_c_3','prov_cnty_c_4':'sum_prov_cnty_c_4','prov_cnty_c_5':'sum_prov_cnty_c_5','prov_cnty_c_6':'sum_prov_cnty_c_6','prov_cnty_c_7':'sum_prov_cnty_c_7','prov_cnty_greq_c_8':'sum_prov_cnty_greq_c_8'})
num_provs_c_county = num_provs_c_county.reset_index()
num_provs_c_county_join_fields = num_provs[['stateFIPS','CountyPop','countyFIPS']].drop_duplicates()
num_provs_c_county = num_provs_c_county.merge(num_provs_c_county_join_fields, how='inner', on='countyFIPS')
num_provs_c_county.to_csv(os.path.join(workdir, r'num_provs_c_county.csv'), encoding='utf-8')

state_pop = num_provs_c_county.groupby(['stateFIPS'], as_index = False)['CountyPop'].sum()
state_pop = state_pop.rename(columns={'CountyPop': 'StatePop'})
num_provs_c_state = num_provs.groupby(['stateFIPS'])["prov_state_c_0","prov_state_c_1","prov_state_c_2","prov_state_c_3","prov_state_c_4","prov_state_c_5","prov_state_c_6","prov_state_c_7","prov_state_greq_c_8"].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_state_c_0':'sum_prov_state_c_0','prov_state_c_1':'sum_prov_state_c_1','prov_state_c_2':'sum_prov_state_c_2','prov_state_c_3':'sum_prov_state_c_3','prov_state_c_4':'sum_prov_state_c_4','prov_state_c_5':'sum_prov_state_c_5','prov_state_c_6':'sum_prov_state_c_6','prov_state_c_7':'sum_prov_state_c_7','prov_state_greq_c_8':'sum_prov_state_greq_c_8'})
num_provs_c_state = num_provs_c_state.reset_index()
num_provs_c_state = num_provs_c_state.merge(state_pop, how='inner', on='stateFIPS')
num_provs_c_state.to_csv(os.path.join(workdir, r'num_provs_c_state.csv'), encoding='utf-8')

num_provs_c_nat = num_provs.groupby(['merge_level'], as_index = False)['prov_nat_c_0','prov_nat_c_1','prov_nat_c_2','prov_nat_c_3','prov_nat_c_4','prov_nat_c_5','prov_nat_c_6','prov_nat_c_7','prov_nat_greq_c_8'].apply(lambda x : x.astype(float).sum()).rename(columns={'prov_nat_c_0':'sum_prov_nat_c_0','prov_nat_c_1':'sum_prov_nat_c_1','prov_nat_c_2':'sum_prov_nat_c_2','prov_nat_c_3':'sum_prov_nat_c_3','prov_nat_c_4':'sum_prov_nat_c_4','prov_nat_c_5':'sum_prov_nat_c_5','prov_nat_c_6':'sum_prov_nat_c_6','prov_nat_c_7':'sum_prov_nat_c_7','prov_nat_greq_c_8':'sum_prov_nat_greq_c_8'})
num_provs_c_nat['merge_level'] = 'national'
num_provs_c_nat.to_csv(os.path.join(workdir, r'num_provs_c_nat.csv'), encoding='utf-8')

import pandas as pd
import signal
import os
import numpy
import functools as func
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'

num_provs_c_county = pd.read_csv('num_provs_c_county.csv')
num_provs_b_county = pd.read_csv('num_provs_b_county.csv')
num_provs_a_county = pd.read_csv('num_provs_a_county.csv')

prov_c = num_provs_c_county.merge(num_provs_b_county, how='inner', on='countyFIPS')
prov_all_counties = prov_c.merge(num_provs_a_county, how ='inner', on='countyFIPS')

num_provs_c_state = pd.read_csv('num_provs_c_state.csv')
num_provs_b_state = pd.read_csv('num_provs_b_state.csv')
num_provs_a_state = pd.read_csv('num_provs_a_state.csv')

prov_s = num_provs_c_state.merge(num_provs_b_state, how='inner', on='stateFIPS')
prov_all_states = prov_s.merge(num_provs_a_state, how ='inner', on='stateFIPS')

num_provs_c_nat = pd.read_csv('num_provs_c_nat.csv')
num_provs_b_nat = pd.read_csv('num_provs_b_nat.csv')
num_provs_a_nat = pd.read_csv('num_provs_a_nat.csv')

prov_n = num_provs_c_nat.merge(num_provs_b_nat, how='inner', on='merge_level')
prov_all_nation = prov_n.merge(num_provs_a_nat, how ='inner', on='merge_level')

prov_all_counties.to_csv(os.path.join(workdir, r'prov_c.csv'), encoding='utf-8')
prov_all_states.to_csv(os.path.join(workdir, r'prov_s.csv'), encoding='utf-8')
prov_all_nation.to_csv(os.path.join(workdir, r'prov_n.csv'), encoding='utf-8')

prov_all_counties = pd.read_csv('prov_c.csv')
prov_all_states = pd.read_csv('prov_s.csv')

prov_cs = prov_all_counties.merge(prov_all_states, how='inner', on='stateFIPS')
prov_cs.to_csv(os.path.join(workdir, r'prov_cs.csv'), encoding='utf-8')

prov_all_states_counties = pd.read_csv('prov_cs.csv')
prov_all_states_counties['merge_level'] = 'national'
prov_all_nation = pd.read_csv('prov_n.csv')
prov_csn = prov_all_states_counties.merge(prov_all_nation, how='inner', on='merge_level')
prov_csn.to_csv(os.path.join(workdir, r'prov_csn.csv'), encoding='utf-8')

num_provs_all = prov_csn
county_data_final_hi_common = pd.read_csv('county_data_final_hi_common.csv')
county_data_num_provs = county_data_final_hi_common.merge(num_provs_all, how='inner', on='countyFIPS')
county_data_cumm_provs = county_data_num_provs.copy()

#Table 61 - Create Copy of county_data_num_provs for Cumulative Provider Columns
county_data_cumm_provs = county_data_num_provs.copy()


## WRITE TABLES FOR NEXT STEP ## 
county_data_cumm_provs.to_csv(os.path.join(workdir, r'county_data_cumm_provs.csv'), encoding='utf-8')

import pandas as pd
import signal
import os
import numpy
import functools as func
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'
pd.options.mode.chained_assignment = None  # default='warn'


county_data_cumm_provs = pd.read_csv('county_data_cumm_provs.csv')
county_data_cumm_provs = county_data_cumm_provs.fillna(0)

county_data_cumm_provs['cumm_prov_cnty_c_0'] = county_data_cumm_provs['sum_prov_cnty_c_0']
county_data_cumm_provs['cumm_prov_cnty_c_1'] = county_data_cumm_provs['cumm_prov_cnty_c_0'] + county_data_cumm_provs['sum_prov_cnty_c_1']
county_data_cumm_provs['cumm_prov_cnty_c_2'] = county_data_cumm_provs['cumm_prov_cnty_c_1'] + county_data_cumm_provs['sum_prov_cnty_c_2']
county_data_cumm_provs['cumm_prov_cnty_c_3'] = county_data_cumm_provs['cumm_prov_cnty_c_2'] + county_data_cumm_provs['sum_prov_cnty_c_3']
county_data_cumm_provs['cumm_prov_cnty_c_4'] = county_data_cumm_provs['cumm_prov_cnty_c_3'] + county_data_cumm_provs['sum_prov_cnty_c_4']
county_data_cumm_provs['cumm_prov_cnty_c_5'] = county_data_cumm_provs['cumm_prov_cnty_c_4'] + county_data_cumm_provs['sum_prov_cnty_c_5']
county_data_cumm_provs['cumm_prov_cnty_c_6'] = county_data_cumm_provs['cumm_prov_cnty_c_5'] + county_data_cumm_provs['sum_prov_cnty_c_6']
county_data_cumm_provs['cumm_prov_cnty_c_7'] = county_data_cumm_provs['cumm_prov_cnty_c_6'] + county_data_cumm_provs['sum_prov_cnty_c_7']
county_data_cumm_provs['cumm_prov_cnty_c_8'] = county_data_cumm_provs['cumm_prov_cnty_c_7'] + county_data_cumm_provs['sum_prov_cnty_greq_c_8']

county_data_cumm_provs['cumm_prov_cnty_b_0'] = county_data_cumm_provs['sum_prov_cnty_b_0']
county_data_cumm_provs['cumm_prov_cnty_b_1'] = county_data_cumm_provs['cumm_prov_cnty_b_0'] + county_data_cumm_provs['sum_prov_cnty_b_1']
county_data_cumm_provs['cumm_prov_cnty_b_2'] = county_data_cumm_provs['cumm_prov_cnty_b_1'] + county_data_cumm_provs['sum_prov_cnty_b_2']
county_data_cumm_provs['cumm_prov_cnty_b_3'] = county_data_cumm_provs['cumm_prov_cnty_b_2'] + county_data_cumm_provs['sum_prov_cnty_b_3']
county_data_cumm_provs['cumm_prov_cnty_b_4'] = county_data_cumm_provs['cumm_prov_cnty_b_3'] + county_data_cumm_provs['sum_prov_cnty_b_4']
county_data_cumm_provs['cumm_prov_cnty_b_5'] = county_data_cumm_provs['cumm_prov_cnty_b_4'] + county_data_cumm_provs['sum_prov_cnty_b_5']
county_data_cumm_provs['cumm_prov_cnty_b_6'] = county_data_cumm_provs['cumm_prov_cnty_b_5'] + county_data_cumm_provs['sum_prov_cnty_b_6']
county_data_cumm_provs['cumm_prov_cnty_b_7'] = county_data_cumm_provs['cumm_prov_cnty_b_6'] + county_data_cumm_provs['sum_prov_cnty_b_7']
county_data_cumm_provs['cumm_prov_cnty_b_8'] = county_data_cumm_provs['cumm_prov_cnty_b_7'] + county_data_cumm_provs['sum_prov_cnty_greq_b_8']

county_data_cumm_provs['cumm_prov_cnty_a_0'] = county_data_cumm_provs['sum_prov_cnty_a_0']
county_data_cumm_provs['cumm_prov_cnty_a_1'] = county_data_cumm_provs['cumm_prov_cnty_a_0'] + county_data_cumm_provs['sum_prov_cnty_a_1']
county_data_cumm_provs['cumm_prov_cnty_a_2'] = county_data_cumm_provs['cumm_prov_cnty_a_1'] + county_data_cumm_provs['sum_prov_cnty_a_2']
county_data_cumm_provs['cumm_prov_cnty_a_3'] = county_data_cumm_provs['cumm_prov_cnty_a_2'] + county_data_cumm_provs['sum_prov_cnty_a_3']
county_data_cumm_provs['cumm_prov_cnty_a_4'] = county_data_cumm_provs['cumm_prov_cnty_a_3'] + county_data_cumm_provs['sum_prov_cnty_a_4']
county_data_cumm_provs['cumm_prov_cnty_a_5'] = county_data_cumm_provs['cumm_prov_cnty_a_4'] + county_data_cumm_provs['sum_prov_cnty_a_5']
county_data_cumm_provs['cumm_prov_cnty_a_6'] = county_data_cumm_provs['cumm_prov_cnty_a_5'] + county_data_cumm_provs['sum_prov_cnty_a_6']
county_data_cumm_provs['cumm_prov_cnty_a_7'] = county_data_cumm_provs['cumm_prov_cnty_a_6'] + county_data_cumm_provs['sum_prov_cnty_a_7']
county_data_cumm_provs['cumm_prov_cnty_a_8'] = county_data_cumm_provs['cumm_prov_cnty_a_7'] + county_data_cumm_provs['sum_prov_cnty_greq_a_8']


county_data_cumm_provs['cumm_prov_state_c_0'] = county_data_cumm_provs['sum_prov_state_c_0']
county_data_cumm_provs['cumm_prov_state_c_1'] = county_data_cumm_provs['cumm_prov_state_c_0'] + county_data_cumm_provs['sum_prov_state_c_1']
county_data_cumm_provs['cumm_prov_state_c_2'] = county_data_cumm_provs['cumm_prov_state_c_1'] + county_data_cumm_provs['sum_prov_state_c_2']
county_data_cumm_provs['cumm_prov_state_c_3'] = county_data_cumm_provs['cumm_prov_state_c_2'] + county_data_cumm_provs['sum_prov_state_c_3']
county_data_cumm_provs['cumm_prov_state_c_4'] = county_data_cumm_provs['cumm_prov_state_c_3'] + county_data_cumm_provs['sum_prov_state_c_4']
county_data_cumm_provs['cumm_prov_state_c_5'] = county_data_cumm_provs['cumm_prov_state_c_4'] + county_data_cumm_provs['sum_prov_state_c_5']
county_data_cumm_provs['cumm_prov_state_c_6'] = county_data_cumm_provs['cumm_prov_state_c_5'] + county_data_cumm_provs['sum_prov_state_c_6']
county_data_cumm_provs['cumm_prov_state_c_7'] = county_data_cumm_provs['cumm_prov_state_c_6'] + county_data_cumm_provs['sum_prov_state_c_7']
county_data_cumm_provs['cumm_prov_state_c_8'] = county_data_cumm_provs['cumm_prov_state_c_7'] + county_data_cumm_provs['sum_prov_state_greq_c_8']

county_data_cumm_provs['cumm_prov_state_b_0'] = county_data_cumm_provs['sum_prov_state_b_0']
county_data_cumm_provs['cumm_prov_state_b_1'] = county_data_cumm_provs['cumm_prov_state_b_0'] + county_data_cumm_provs['sum_prov_state_b_1']
county_data_cumm_provs['cumm_prov_state_b_2'] = county_data_cumm_provs['cumm_prov_state_b_1'] + county_data_cumm_provs['sum_prov_state_b_2']
county_data_cumm_provs['cumm_prov_state_b_3'] = county_data_cumm_provs['cumm_prov_state_b_2'] + county_data_cumm_provs['sum_prov_state_b_3']
county_data_cumm_provs['cumm_prov_state_b_4'] = county_data_cumm_provs['cumm_prov_state_b_3'] + county_data_cumm_provs['sum_prov_state_b_4']
county_data_cumm_provs['cumm_prov_state_b_5'] = county_data_cumm_provs['cumm_prov_state_b_4'] + county_data_cumm_provs['sum_prov_state_b_5']
county_data_cumm_provs['cumm_prov_state_b_6'] = county_data_cumm_provs['cumm_prov_state_b_5'] + county_data_cumm_provs['sum_prov_state_b_6']
county_data_cumm_provs['cumm_prov_state_b_7'] = county_data_cumm_provs['cumm_prov_state_b_6'] + county_data_cumm_provs['sum_prov_state_b_7']
county_data_cumm_provs['cumm_prov_state_b_8'] = county_data_cumm_provs['cumm_prov_state_b_7'] + county_data_cumm_provs['sum_prov_state_greq_b_8']

county_data_cumm_provs['cumm_prov_state_a_0'] = county_data_cumm_provs['sum_prov_state_a_0']
county_data_cumm_provs['cumm_prov_state_a_1'] = county_data_cumm_provs['cumm_prov_state_a_0'] + county_data_cumm_provs['sum_prov_state_a_1']
county_data_cumm_provs['cumm_prov_state_a_2'] = county_data_cumm_provs['cumm_prov_state_a_1'] + county_data_cumm_provs['sum_prov_state_a_2']
county_data_cumm_provs['cumm_prov_state_a_3'] = county_data_cumm_provs['cumm_prov_state_a_2'] + county_data_cumm_provs['sum_prov_state_a_3']
county_data_cumm_provs['cumm_prov_state_a_4'] = county_data_cumm_provs['cumm_prov_state_a_3'] + county_data_cumm_provs['sum_prov_state_a_4']
county_data_cumm_provs['cumm_prov_state_a_5'] = county_data_cumm_provs['cumm_prov_state_a_4'] + county_data_cumm_provs['sum_prov_state_a_5']
county_data_cumm_provs['cumm_prov_state_a_6'] = county_data_cumm_provs['cumm_prov_state_a_5'] + county_data_cumm_provs['sum_prov_state_a_6']
county_data_cumm_provs['cumm_prov_state_a_7'] = county_data_cumm_provs['cumm_prov_state_a_6'] + county_data_cumm_provs['sum_prov_state_a_7']
county_data_cumm_provs['cumm_prov_state_a_8'] = county_data_cumm_provs['cumm_prov_state_a_7'] + county_data_cumm_provs['sum_prov_state_greq_a_8']

county_data_cumm_provs['cumm_prov_nat_c_0'] = county_data_cumm_provs['sum_prov_nat_c_0']
county_data_cumm_provs['cumm_prov_nat_c_1'] = county_data_cumm_provs['cumm_prov_nat_c_0'] + county_data_cumm_provs['sum_prov_nat_c_1']
county_data_cumm_provs['cumm_prov_nat_c_2'] = county_data_cumm_provs['cumm_prov_nat_c_1'] + county_data_cumm_provs['sum_prov_nat_c_2']
county_data_cumm_provs['cumm_prov_nat_c_3'] = county_data_cumm_provs['cumm_prov_nat_c_2'] + county_data_cumm_provs['sum_prov_nat_c_3']
county_data_cumm_provs['cumm_prov_nat_c_4'] = county_data_cumm_provs['cumm_prov_nat_c_3'] + county_data_cumm_provs['sum_prov_nat_c_4']
county_data_cumm_provs['cumm_prov_nat_c_5'] = county_data_cumm_provs['cumm_prov_nat_c_4'] + county_data_cumm_provs['sum_prov_nat_c_5']
county_data_cumm_provs['cumm_prov_nat_c_6'] = county_data_cumm_provs['cumm_prov_nat_c_5'] + county_data_cumm_provs['sum_prov_nat_c_6']
county_data_cumm_provs['cumm_prov_nat_c_7'] = county_data_cumm_provs['cumm_prov_nat_c_6'] + county_data_cumm_provs['sum_prov_nat_c_7']
county_data_cumm_provs['cumm_prov_nat_c_8'] = county_data_cumm_provs['cumm_prov_nat_c_7'] + county_data_cumm_provs['sum_prov_nat_greq_c_8']

county_data_cumm_provs['cumm_prov_nat_b_0'] = county_data_cumm_provs['sum_prov_nat_b_0']
county_data_cumm_provs['cumm_prov_nat_b_1'] = county_data_cumm_provs['cumm_prov_nat_b_0'] + county_data_cumm_provs['sum_prov_nat_b_1']
county_data_cumm_provs['cumm_prov_nat_b_2'] = county_data_cumm_provs['cumm_prov_nat_b_1'] + county_data_cumm_provs['sum_prov_nat_b_2']
county_data_cumm_provs['cumm_prov_nat_b_3'] = county_data_cumm_provs['cumm_prov_nat_b_2'] + county_data_cumm_provs['sum_prov_nat_b_3']
county_data_cumm_provs['cumm_prov_nat_b_4'] = county_data_cumm_provs['cumm_prov_nat_b_3'] + county_data_cumm_provs['sum_prov_nat_b_4']
county_data_cumm_provs['cumm_prov_nat_b_5'] = county_data_cumm_provs['cumm_prov_nat_b_4'] + county_data_cumm_provs['sum_prov_nat_b_5']
county_data_cumm_provs['cumm_prov_nat_b_6'] = county_data_cumm_provs['cumm_prov_nat_b_5'] + county_data_cumm_provs['sum_prov_nat_b_6']
county_data_cumm_provs['cumm_prov_nat_b_7'] = county_data_cumm_provs['cumm_prov_nat_b_6'] + county_data_cumm_provs['sum_prov_nat_b_7']
county_data_cumm_provs['cumm_prov_nat_b_8'] = county_data_cumm_provs['cumm_prov_nat_b_7'] + county_data_cumm_provs['sum_prov_nat_greq_b_8']

county_data_cumm_provs['cumm_prov_nat_a_0'] = county_data_cumm_provs['sum_prov_nat_a_0']
county_data_cumm_provs['cumm_prov_nat_a_1'] = county_data_cumm_provs['cumm_prov_nat_a_0'] + county_data_cumm_provs['sum_prov_nat_a_1']
county_data_cumm_provs['cumm_prov_nat_a_2'] = county_data_cumm_provs['cumm_prov_nat_a_1'] + county_data_cumm_provs['sum_prov_nat_a_2']
county_data_cumm_provs['cumm_prov_nat_a_3'] = county_data_cumm_provs['cumm_prov_nat_a_2'] + county_data_cumm_provs['sum_prov_nat_a_3']
county_data_cumm_provs['cumm_prov_nat_a_4'] = county_data_cumm_provs['cumm_prov_nat_a_3'] + county_data_cumm_provs['sum_prov_nat_a_4']
county_data_cumm_provs['cumm_prov_nat_a_5'] = county_data_cumm_provs['cumm_prov_nat_a_4'] + county_data_cumm_provs['sum_prov_nat_a_5']
county_data_cumm_provs['cumm_prov_nat_a_6'] = county_data_cumm_provs['cumm_prov_nat_a_5'] + county_data_cumm_provs['sum_prov_nat_a_6']
county_data_cumm_provs['cumm_prov_nat_a_7'] = county_data_cumm_provs['cumm_prov_nat_a_6'] + county_data_cumm_provs['sum_prov_nat_a_7']
county_data_cumm_provs['cumm_prov_nat_a_8'] = county_data_cumm_provs['cumm_prov_nat_a_7'] + county_data_cumm_provs['sum_prov_nat_greq_a_8']


county_data_cumm_provs['lb'] = ''
county_data_cumm_provs['cumm_prov_cnty_c_50th'] = ''

county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_1'] >= 0.5), 'lb'] = 'cumm_prov_cnty_c_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_1'] >= 0.5), 'cumm_prov_cnty_c_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_c_0']) / (county_data_cumm_provs['cumm_prov_cnty_c_1'] - county_data_cumm_provs['cumm_prov_cnty_c_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_2'] >= 0.5), 'lb'] = 'cumm_prov_cnty_c_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_2'] >= 0.5), 'cumm_prov_cnty_c_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_c_1']) / (county_data_cumm_provs['cumm_prov_cnty_c_2'] - county_data_cumm_provs['cumm_prov_cnty_c_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_3'] >= 0.5), 'lb'] = 'cumm_prov_cnty_c_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_3'] >= 0.5), 'cumm_prov_cnty_c_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_c_2']) / (county_data_cumm_provs['cumm_prov_cnty_c_3'] - county_data_cumm_provs['cumm_prov_cnty_c_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_4'] >= 0.5), 'lb'] = 'cumm_prov_cnty_c_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_4'] >= 0.5), 'cumm_prov_cnty_c_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_c_3']) / (county_data_cumm_provs['cumm_prov_cnty_c_4'] - county_data_cumm_provs['cumm_prov_cnty_c_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_5'] >= 0.5), 'lb'] = 'cumm_prov_cnty_c_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_5'] >= 0.5), 'cumm_prov_cnty_c_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_c_4']) / (county_data_cumm_provs['cumm_prov_cnty_c_5'] - county_data_cumm_provs['cumm_prov_cnty_c_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_6'] >= 0.5), 'lb'] = 'cumm_prov_cnty_c_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_6'] >= 0.5), 'cumm_prov_cnty_c_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_c_5']) / (county_data_cumm_provs['cumm_prov_cnty_c_6'] - county_data_cumm_provs['cumm_prov_cnty_c_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_7'] >= 0.5), 'lb'] = 'cumm_prov_cnty_c_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_7'] >= 0.5), 'cumm_prov_cnty_c_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_c_6']) / (county_data_cumm_provs['cumm_prov_cnty_c_7'] - county_data_cumm_provs['cumm_prov_cnty_c_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_8'] >= 0.5), 'lb'] = 'cumm_prov_cnty_c_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_c_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_c_8'] >= 0.5), 'cumm_prov_cnty_c_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_c_7']) / (county_data_cumm_provs['cumm_prov_cnty_c_8'] - county_data_cumm_provs['cumm_prov_cnty_c_7']))


county_data_cumm_provs['cumm_prov_cnty_b_50th'] = ''
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_1'] >= 0.5), 'lb'] = 'cumm_prov_cnty_b_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_1'] >= 0.5), 'cumm_prov_cnty_b_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_b_0']) / (county_data_cumm_provs['cumm_prov_cnty_b_1'] - county_data_cumm_provs['cumm_prov_cnty_b_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_2'] >= 0.5), 'lb'] = 'cumm_prov_cnty_b_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_2'] >= 0.5), 'cumm_prov_cnty_b_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_b_1']) / (county_data_cumm_provs['cumm_prov_cnty_b_2'] - county_data_cumm_provs['cumm_prov_cnty_b_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_3'] >= 0.5), 'lb'] = 'cumm_prov_cnty_b_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_3'] >= 0.5), 'cumm_prov_cnty_b_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_b_2']) / (county_data_cumm_provs['cumm_prov_cnty_b_3'] - county_data_cumm_provs['cumm_prov_cnty_b_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_4'] >= 0.5), 'lb'] = 'cumm_prov_cnty_b_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_4'] >= 0.5), 'cumm_prov_cnty_b_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_b_3']) / (county_data_cumm_provs['cumm_prov_cnty_b_4'] - county_data_cumm_provs['cumm_prov_cnty_b_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_5'] >= 0.5), 'lb'] = 'cumm_prov_cnty_b_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_5'] >= 0.5), 'cumm_prov_cnty_b_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_b_4']) / (county_data_cumm_provs['cumm_prov_cnty_b_5'] - county_data_cumm_provs['cumm_prov_cnty_b_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_6'] >= 0.5), 'lb'] = 'cumm_prov_cnty_b_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_6'] >= 0.5), 'cumm_prov_cnty_b_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_b_5']) / (county_data_cumm_provs['cumm_prov_cnty_b_6'] - county_data_cumm_provs['cumm_prov_cnty_b_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_7'] >= 0.5), 'lb'] = 'cumm_prov_cnty_b_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_7'] >= 0.5), 'cumm_prov_cnty_b_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_b_6']) / (county_data_cumm_provs['cumm_prov_cnty_b_7'] - county_data_cumm_provs['cumm_prov_cnty_b_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_8'] >= 0.5), 'lb'] = 'cumm_prov_cnty_b_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_b_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_b_8'] >= 0.5), 'cumm_prov_cnty_b_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_b_7']) / (county_data_cumm_provs['cumm_prov_cnty_b_8'] - county_data_cumm_provs['cumm_prov_cnty_b_7']))

county_data_cumm_provs['cumm_prov_cnty_a_50th'] = ''
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_1'] >= 0.5), 'lb'] = 'cumm_prov_cnty_a_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_1'] >= 0.5), 'cumm_prov_cnty_a_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_a_0']) / (county_data_cumm_provs['cumm_prov_cnty_a_1'] - county_data_cumm_provs['cumm_prov_cnty_a_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_2'] >= 0.5), 'lb'] = 'cumm_prov_cnty_a_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_2'] >= 0.5), 'cumm_prov_cnty_a_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_a_1']) / (county_data_cumm_provs['cumm_prov_cnty_a_2'] - county_data_cumm_provs['cumm_prov_cnty_a_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_3'] >= 0.5), 'lb'] = 'cumm_prov_cnty_a_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_3'] >= 0.5), 'cumm_prov_cnty_a_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_a_2']) / (county_data_cumm_provs['cumm_prov_cnty_a_3'] - county_data_cumm_provs['cumm_prov_cnty_a_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_4'] >= 0.5), 'lb'] = 'cumm_prov_cnty_a_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_4'] >= 0.5), 'cumm_prov_cnty_a_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_a_3']) / (county_data_cumm_provs['cumm_prov_cnty_a_4'] - county_data_cumm_provs['cumm_prov_cnty_a_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_5'] >= 0.5), 'lb'] = 'cumm_prov_cnty_a_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_5'] >= 0.5), 'cumm_prov_cnty_a_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_a_4']) / (county_data_cumm_provs['cumm_prov_cnty_a_5'] - county_data_cumm_provs['cumm_prov_cnty_a_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_6'] >= 0.5), 'lb'] = 'cumm_prov_cnty_a_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_6'] >= 0.5), 'cumm_prov_cnty_a_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_a_5']) / (county_data_cumm_provs['cumm_prov_cnty_a_6'] - county_data_cumm_provs['cumm_prov_cnty_a_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_7'] >= 0.5), 'lb'] = 'cumm_prov_cnty_a_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_7'] >= 0.5), 'cumm_prov_cnty_a_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_a_6']) / (county_data_cumm_provs['cumm_prov_cnty_a_7'] - county_data_cumm_provs['cumm_prov_cnty_a_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_8'] >= 0.5), 'lb'] = 'cumm_prov_cnty_a_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_cnty_a_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_cnty_a_8'] >= 0.5), 'cumm_prov_cnty_a_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_cnty_a_7']) / (county_data_cumm_provs['cumm_prov_cnty_a_8'] - county_data_cumm_provs['cumm_prov_cnty_a_7']))

county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_1'] >= 0.5), 'lb'] = 'cumm_prov_state_c_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_1'] >= 0.5), 'cumm_prov_state_c_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_state_c_0']) / (county_data_cumm_provs['cumm_prov_state_c_1'] - county_data_cumm_provs['cumm_prov_state_c_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_2'] >= 0.5), 'lb'] = 'cumm_prov_state_c_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_2'] >= 0.5), 'cumm_prov_state_c_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_state_c_1']) / (county_data_cumm_provs['cumm_prov_state_c_2'] - county_data_cumm_provs['cumm_prov_state_c_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_3'] >= 0.5), 'lb'] = 'cumm_prov_state_c_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_3'] >= 0.5), 'cumm_prov_state_c_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_state_c_2']) / (county_data_cumm_provs['cumm_prov_state_c_3'] - county_data_cumm_provs['cumm_prov_state_c_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_4'] >= 0.5), 'lb'] = 'cumm_prov_state_c_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_4'] >= 0.5), 'cumm_prov_state_c_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_state_c_3']) / (county_data_cumm_provs['cumm_prov_state_c_4'] - county_data_cumm_provs['cumm_prov_state_c_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_5'] >= 0.5), 'lb'] = 'cumm_prov_state_c_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_5'] >= 0.5), 'cumm_prov_state_c_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_state_c_4']) / (county_data_cumm_provs['cumm_prov_state_c_5'] - county_data_cumm_provs['cumm_prov_state_c_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_6'] >= 0.5), 'lb'] = 'cumm_prov_state_c_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_6'] >= 0.5), 'cumm_prov_state_c_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_state_c_5']) / (county_data_cumm_provs['cumm_prov_state_c_6'] - county_data_cumm_provs['cumm_prov_state_c_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_7'] >= 0.5), 'lb'] = 'cumm_prov_state_c_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_7'] >= 0.5), 'cumm_prov_state_c_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_state_c_6']) / (county_data_cumm_provs['cumm_prov_state_c_7'] - county_data_cumm_provs['cumm_prov_state_c_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_8'] >= 0.5), 'lb'] = 'cumm_prov_state_c_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_c_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_c_8'] >= 0.5), 'cumm_prov_state_c_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_state_c_7']) / (county_data_cumm_provs['cumm_prov_state_c_8'] - county_data_cumm_provs['cumm_prov_state_c_7']))


county_data_cumm_provs['cumm_prov_state_b_50th'] = ''
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_1'] >= 0.5), 'lb'] = 'cumm_prov_state_b_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_1'] >= 0.5), 'cumm_prov_state_b_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_state_b_0']) / (county_data_cumm_provs['cumm_prov_state_b_1'] - county_data_cumm_provs['cumm_prov_state_b_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_2'] >= 0.5), 'lb'] = 'cumm_prov_state_b_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_2'] >= 0.5), 'cumm_prov_state_b_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_state_b_1']) / (county_data_cumm_provs['cumm_prov_state_b_2'] - county_data_cumm_provs['cumm_prov_state_b_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_3'] >= 0.5), 'lb'] = 'cumm_prov_state_b_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_3'] >= 0.5), 'cumm_prov_state_b_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_state_b_2']) / (county_data_cumm_provs['cumm_prov_state_b_3'] - county_data_cumm_provs['cumm_prov_state_b_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_4'] >= 0.5), 'lb'] = 'cumm_prov_state_b_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_4'] >= 0.5), 'cumm_prov_state_b_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_state_b_3']) / (county_data_cumm_provs['cumm_prov_state_b_4'] - county_data_cumm_provs['cumm_prov_state_b_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_5'] >= 0.5), 'lb'] = 'cumm_prov_state_b_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_5'] >= 0.5), 'cumm_prov_state_b_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_state_b_4']) / (county_data_cumm_provs['cumm_prov_state_b_5'] - county_data_cumm_provs['cumm_prov_state_b_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_6'] >= 0.5), 'lb'] = 'cumm_prov_state_b_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_6'] >= 0.5), 'cumm_prov_state_b_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_state_b_5']) / (county_data_cumm_provs['cumm_prov_state_b_6'] - county_data_cumm_provs['cumm_prov_state_b_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_7'] >= 0.5), 'lb'] = 'cumm_prov_state_b_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_7'] >= 0.5), 'cumm_prov_state_b_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_state_b_6']) / (county_data_cumm_provs['cumm_prov_state_b_7'] - county_data_cumm_provs['cumm_prov_state_b_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_8'] >= 0.5), 'lb'] = 'cumm_prov_state_b_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_b_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_b_8'] >= 0.5), 'cumm_prov_state_b_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_state_b_7']) / (county_data_cumm_provs['cumm_prov_state_b_8'] - county_data_cumm_provs['cumm_prov_state_b_7']))

county_data_cumm_provs['cumm_prov_state_a_50th'] = ''
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_1'] >= 0.5), 'lb'] = 'cumm_prov_state_a_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_1'] >= 0.5), 'cumm_prov_state_a_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_state_a_0']) / (county_data_cumm_provs['cumm_prov_state_a_1'] - county_data_cumm_provs['cumm_prov_state_a_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_2'] >= 0.5), 'lb'] = 'cumm_prov_state_a_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_2'] >= 0.5), 'cumm_prov_state_a_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_state_a_1']) / (county_data_cumm_provs['cumm_prov_state_a_2'] - county_data_cumm_provs['cumm_prov_state_a_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_3'] >= 0.5), 'lb'] = 'cumm_prov_state_a_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_3'] >= 0.5), 'cumm_prov_state_a_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_state_a_2']) / (county_data_cumm_provs['cumm_prov_state_a_3'] - county_data_cumm_provs['cumm_prov_state_a_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_4'] >= 0.5), 'lb'] = 'cumm_prov_state_a_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_4'] >= 0.5), 'cumm_prov_state_a_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_state_a_3']) / (county_data_cumm_provs['cumm_prov_state_a_4'] - county_data_cumm_provs['cumm_prov_state_a_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_5'] >= 0.5), 'lb'] = 'cumm_prov_state_a_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_5'] >= 0.5), 'cumm_prov_state_a_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_state_a_4']) / (county_data_cumm_provs['cumm_prov_state_a_5'] - county_data_cumm_provs['cumm_prov_state_a_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_6'] >= 0.5), 'lb'] = 'cumm_prov_state_a_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_6'] >= 0.5), 'cumm_prov_state_a_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_state_a_5']) / (county_data_cumm_provs['cumm_prov_state_a_6'] - county_data_cumm_provs['cumm_prov_state_a_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_7'] >= 0.5), 'lb'] = 'cumm_prov_state_a_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_7'] >= 0.5), 'cumm_prov_state_a_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_state_a_6']) / (county_data_cumm_provs['cumm_prov_state_a_7'] - county_data_cumm_provs['cumm_prov_state_a_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_8'] >= 0.5), 'lb'] = 'cumm_prov_state_a_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_state_a_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_state_a_8'] >= 0.5), 'cumm_prov_state_a_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_state_a_7']) / (county_data_cumm_provs['cumm_prov_state_a_8'] - county_data_cumm_provs['cumm_prov_state_a_7']))

county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_1'] >= 0.5), 'lb'] = 'cumm_prov_nat_c_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_1'] >= 0.5), 'cumm_prov_nat_c_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_c_0']) / (county_data_cumm_provs['cumm_prov_nat_c_1'] - county_data_cumm_provs['cumm_prov_nat_c_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_2'] >= 0.5), 'lb'] = 'cumm_prov_nat_c_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_2'] >= 0.5), 'cumm_prov_nat_c_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_c_1']) / (county_data_cumm_provs['cumm_prov_nat_c_2'] - county_data_cumm_provs['cumm_prov_nat_c_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_3'] >= 0.5), 'lb'] = 'cumm_prov_nat_c_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_3'] >= 0.5), 'cumm_prov_nat_c_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_c_2']) / (county_data_cumm_provs['cumm_prov_nat_c_3'] - county_data_cumm_provs['cumm_prov_nat_c_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_4'] >= 0.5), 'lb'] = 'cumm_prov_nat_c_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_4'] >= 0.5), 'cumm_prov_nat_c_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_c_3']) / (county_data_cumm_provs['cumm_prov_nat_c_4'] - county_data_cumm_provs['cumm_prov_nat_c_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_5'] >= 0.5), 'lb'] = 'cumm_prov_nat_c_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_5'] >= 0.5), 'cumm_prov_nat_c_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_c_4']) / (county_data_cumm_provs['cumm_prov_nat_c_5'] - county_data_cumm_provs['cumm_prov_nat_c_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_6'] >= 0.5), 'lb'] = 'cumm_prov_nat_c_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_6'] >= 0.5), 'cumm_prov_nat_c_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_c_5']) / (county_data_cumm_provs['cumm_prov_nat_c_6'] - county_data_cumm_provs['cumm_prov_nat_c_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_7'] >= 0.5), 'lb'] = 'cumm_prov_nat_c_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_7'] >= 0.5), 'cumm_prov_nat_c_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_c_6']) / (county_data_cumm_provs['cumm_prov_nat_c_7'] - county_data_cumm_provs['cumm_prov_nat_c_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_8'] >= 0.5), 'lb'] = 'cumm_prov_nat_c_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_c_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_c_8'] >= 0.5), 'cumm_prov_nat_c_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_c_7']) / (county_data_cumm_provs['cumm_prov_nat_c_8'] - county_data_cumm_provs['cumm_prov_nat_c_7']))


county_data_cumm_provs['cumm_prov_nat_b_50th'] = ''
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_1'] >= 0.5), 'lb'] = 'cumm_prov_nat_b_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_1'] >= 0.5), 'cumm_prov_nat_b_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_b_0']) / (county_data_cumm_provs['cumm_prov_nat_b_1'] - county_data_cumm_provs['cumm_prov_nat_b_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_2'] >= 0.5), 'lb'] = 'cumm_prov_nat_b_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_2'] >= 0.5), 'cumm_prov_nat_b_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_b_1']) / (county_data_cumm_provs['cumm_prov_nat_b_2'] - county_data_cumm_provs['cumm_prov_nat_b_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_3'] >= 0.5), 'lb'] = 'cumm_prov_nat_b_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_3'] >= 0.5), 'cumm_prov_nat_b_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_b_2']) / (county_data_cumm_provs['cumm_prov_nat_b_3'] - county_data_cumm_provs['cumm_prov_nat_b_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_4'] >= 0.5), 'lb'] = 'cumm_prov_nat_b_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_4'] >= 0.5), 'cumm_prov_nat_b_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_b_3']) / (county_data_cumm_provs['cumm_prov_nat_b_4'] - county_data_cumm_provs['cumm_prov_nat_b_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_5'] >= 0.5), 'lb'] = 'cumm_prov_nat_b_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_5'] >= 0.5), 'cumm_prov_nat_b_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_b_4']) / (county_data_cumm_provs['cumm_prov_nat_b_5'] - county_data_cumm_provs['cumm_prov_nat_b_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_6'] >= 0.5), 'lb'] = 'cumm_prov_nat_b_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_6'] >= 0.5), 'cumm_prov_nat_b_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_b_5']) / (county_data_cumm_provs['cumm_prov_nat_b_6'] - county_data_cumm_provs['cumm_prov_nat_b_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_7'] >= 0.5), 'lb'] = 'cumm_prov_nat_b_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_7'] >= 0.5), 'cumm_prov_nat_b_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_b_6']) / (county_data_cumm_provs['cumm_prov_nat_b_7'] - county_data_cumm_provs['cumm_prov_nat_b_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_8'] >= 0.5), 'lb'] = 'cumm_prov_nat_b_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_b_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_b_8'] >= 0.5), 'cumm_prov_nat_b_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_b_7']) / (county_data_cumm_provs['cumm_prov_nat_b_8'] - county_data_cumm_provs['cumm_prov_nat_b_7']))

county_data_cumm_provs['cumm_prov_nat_a_50th'] = ''
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_1'] >= 0.5), 'lb'] = 'cumm_prov_nat_a_0'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_0'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_1'] >= 0.5), 'cumm_prov_nat_a_50th'] = 0 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_a_0']) / (county_data_cumm_provs['cumm_prov_nat_a_1'] - county_data_cumm_provs['cumm_prov_nat_a_0']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_2'] >= 0.5), 'lb'] = 'cumm_prov_nat_a_1'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_1'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_2'] >= 0.5), 'cumm_prov_nat_a_50th'] = 1 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_a_1']) / (county_data_cumm_provs['cumm_prov_nat_a_2'] - county_data_cumm_provs['cumm_prov_nat_a_1']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_3'] >= 0.5), 'lb'] = 'cumm_prov_nat_a_2'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_2'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_3'] >= 0.5), 'cumm_prov_nat_a_50th'] = 2 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_a_2']) / (county_data_cumm_provs['cumm_prov_nat_a_3'] - county_data_cumm_provs['cumm_prov_nat_a_2']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_4'] >= 0.5), 'lb'] = 'cumm_prov_nat_a_3'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_3'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_4'] >= 0.5), 'cumm_prov_nat_a_50th'] = 3 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_a_3']) / (county_data_cumm_provs['cumm_prov_nat_a_4'] - county_data_cumm_provs['cumm_prov_nat_a_3']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_5'] >= 0.5), 'lb'] = 'cumm_prov_nat_a_4'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_4'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_5'] >= 0.5), 'cumm_prov_nat_a_50th'] = 4 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_a_4']) / (county_data_cumm_provs['cumm_prov_nat_a_5'] - county_data_cumm_provs['cumm_prov_nat_a_4']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_6'] >= 0.5), 'lb'] = 'cumm_prov_nat_a_5'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_5'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_6'] >= 0.5), 'cumm_prov_nat_a_50th'] = 5 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_a_5']) / (county_data_cumm_provs['cumm_prov_nat_a_6'] - county_data_cumm_provs['cumm_prov_nat_a_5']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_7'] >= 0.5), 'lb'] = 'cumm_prov_nat_a_6'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_6'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_7'] >= 0.5), 'cumm_prov_nat_a_50th'] = 6 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_a_6']) / (county_data_cumm_provs['cumm_prov_nat_a_7'] - county_data_cumm_provs['cumm_prov_nat_a_6']))
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_8'] >= 0.5), 'lb'] = 'cumm_prov_nat_a_7'
county_data_cumm_provs.loc[(county_data_cumm_provs['cumm_prov_nat_a_7'] < 0.5) & (county_data_cumm_provs['cumm_prov_nat_a_8'] >= 0.5), 'cumm_prov_nat_a_50th'] = 7 + ((0.5 - county_data_cumm_provs['cumm_prov_nat_a_7']) / (county_data_cumm_provs['cumm_prov_nat_a_8'] - county_data_cumm_provs['cumm_prov_nat_a_7']))


county_data_cumm_provs['dsgteq25_c'] = county_data_cumm_provs['pctDSGt25000kAndLt50000k_hi_c'] + county_data_cumm_provs['pctDSGt50000kAndLt100000k_hi_c'] + county_data_cumm_provs['pctDSGt100000kAndLt1Gig_hi_c'] + county_data_cumm_provs['pctDSGt1Gig_hi_c']
county_data_cumm_provs['dsgteq25_s'] = county_data_cumm_provs['pctDSGt25000kAndLt50000k_hi_s'] + county_data_cumm_provs['pctDSGt50000kAndLt100000k_hi_s'] + county_data_cumm_provs['pctDSGt100000kAndLt1Gig_hi_s'] + county_data_cumm_provs['pctDSGt1Gig_hi_s']
county_data_cumm_provs['dsgteq25_n'] = county_data_cumm_provs['pctDSGt25000kAndLt50000k_hi_n'] + county_data_cumm_provs['pctDSGt50000kAndLt100000k_hi_n'] + county_data_cumm_provs['pctDSGt100000kAndLt1Gig_hi_n'] + county_data_cumm_provs['pctDSGt1Gig_hi_n']


county_data_cumm_provs['usgteq3_c'] = county_data_cumm_provs['pctUSGt3000kAndLt4000k_hi_c'] + county_data_cumm_provs['pctUSGt4000kAndLt6000k_hi_c'] + county_data_cumm_provs['pctUSGt6000kAndLt10000k_hi_c'] + county_data_cumm_provs['pctUSGt10000kAndLt15000k_hi_c'] + county_data_cumm_provs['pctUSGt15000kAndLt25000k_hi_c'] + county_data_cumm_provs['pctUSGt25000kAndLt50000k_hi_c'] + county_data_cumm_provs['pctUSGt50000kAndLt100000k_hi_c'] + county_data_cumm_provs['pctUSGt100000kAndLt1Gig_hi_c'] + county_data_cumm_provs['pctUSGt1Gig_hi_c']
county_data_cumm_provs['usgteq3_s'] = county_data_cumm_provs['pctUSGt3000kAndLt4000k_hi_s'] + county_data_cumm_provs['pctUSGt4000kAndLt6000k_hi_s'] + county_data_cumm_provs['pctUSGt6000kAndLt10000k_hi_s'] + county_data_cumm_provs['pctUSGt10000kAndLt15000k_hi_s'] + county_data_cumm_provs['pctUSGt15000kAndLt25000k_hi_s'] + county_data_cumm_provs['pctUSGt25000kAndLt50000k_hi_s'] + county_data_cumm_provs['pctUSGt50000kAndLt100000k_hi_s'] + county_data_cumm_provs['pctUSGt100000kAndLt1Gig_hi_s'] + county_data_cumm_provs['pctUSGt1Gig_hi_s']
county_data_cumm_provs['usgteq3_n'] = county_data_cumm_provs['pctUSGt3000kAndLt4000k_hi_n'] + county_data_cumm_provs['pctUSGt4000kAndLt6000k_hi_n'] + county_data_cumm_provs['pctUSGt6000kAndLt10000k_hi_n'] + county_data_cumm_provs['pctUSGt10000kAndLt15000k_hi_n'] + county_data_cumm_provs['pctUSGt15000kAndLt25000k_hi_n'] + county_data_cumm_provs['pctUSGt25000kAndLt50000k_hi_n'] + county_data_cumm_provs['pctUSGt50000kAndLt100000k_hi_n'] + county_data_cumm_provs['pctUSGt100000kAndLt1Gig_hi_n'] + county_data_cumm_provs['pctUSGt1Gig_hi_n']



county_data_cumm_provs.to_csv(os.path.join(workdir, r'county_data_cumm_provs.csv'), encoding='utf-8')

import pandas as pd
import signal
import os
import numpy
import functools as func
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'

county_data_cumm_provs = pd.read_csv('county_data_cumm_provs.csv')
county_data_cumm_provs['merge_level'] = 'national'


adoption_dec2016_county = pd.read_csv('adoption_dec2016_county.csv')
adoption_dec2016_state = pd.read_csv('adoption_dec2016_state.csv')
adoption_dec2016_nat = pd.read_csv('adoption_dec2016_nat.csv')


county_data_cumm_provs = county_data_cumm_provs.merge(adoption_dec2016_county, how='left', on = 'countyFIPS')
county_data_cumm_provs = county_data_cumm_provs.merge(adoption_dec2016_state, how='left', on = 'stateFIPS')
county_data_cumm_provs = county_data_cumm_provs.merge(adoption_dec2016_nat, how='left', on = 'merge_level')
county_data_cumm_provs.to_csv(os.path.join(workdir, r'accessandadoption2016.csv'))
accessandadoption2016 = county_data_cumm_provs

#Table 67 - Read in Broadband Progress Report

bpr_2018_c = pd.read_csv(r'bpr_2018_c.csv', encoding = 'latin-1' )
bpr_2018_s = pd.read_csv(r'bpr_2018_s.csv', encoding = 'latin-1' )
bpr_2018_n = pd.read_csv(r'bpr_2018_n.csv', encoding = 'latin-1' )

accessandadoption2016 = accessandadoption2016.merge(bpr_2018_c, how='left', on='countyFIPS')
accessandadoption2016 = accessandadoption2016.merge(bpr_2018_s, how='left', on='stateFIPS')
accessandadoption2016 = accessandadoption2016.merge(bpr_2018_n, how='left', on='merge_level')

accessandadoption2016.to_csv(os.path.join(workdir, r'accessandadoption2016_bpr.csv'))

accessandadoption_2016 = pd.read_csv('accessandadoption2016_bpr.csv', low_memory=False)
accessandadoption_2016['less_than_zero'] = ''

if (accessandadoption_2016['pctpopwoBBacc_county'] < 0).any():
    accessandadoption_2016['less_than_zero'] = 1
else:
    accessandadoption_2016['less_than_zero'] = 0
    
accessandadoption_2016['totalpct'] = accessandadoption_2016['pctpopwoBBacc_county'] + accessandadoption_2016['pctpopwBBacc_county']

income_c = pd.read_csv(r'SAIPE_income_c.csv', skiprows =3)
income_c = income_c[['State FIPS Code','County FIPS Code','Postal Code','Poverty Estimate, All Ages','Median Household Income']].rename(columns={'countyFIPS':'countyFIPS_int','stateFIPS':'stateFIPS_int','Postal Code':'PostalCode','Poverty Estimate, All Ages':'poverty_allages_c','Median Household Income':'medianHHinc_c'})
income_c['County FIPS Code'] = income_c['County FIPS Code'].apply(lambda x: '{0:0>3}'.format(x))
income_c['countyFIPS'] = income_c['State FIPS Code'].astype(str)+income_c['County FIPS Code'].astype(str)
income_c['countyFIPS'] = income_c['countyFIPS'].astype(int)
income_c = income_c[['countyFIPS', 'poverty_allages_c', 'medianHHinc_c']]
income_c.to_csv(os.path.join(workdir, r'income_c2.csv'))
accessandadoption_2016 = income_c.merge(accessandadoption_2016, how='left', on='countyFIPS')

income_s = pd.read_csv(r'SAIPE_income_s.csv', skiprows =3)
income_s = income_s[['State FIPS Code','Poverty Estimate, All Ages','Median Household Income']].rename(columns={'State FIPS Code':'stateFIPS','Poverty Estimate, All Ages':'poverty_allages_s','Median Household Income':'medianHHinc_s'})
accessandadoption_2016 = income_s.merge(accessandadoption_2016, how='left', on='stateFIPS')

income_n = pd.read_csv(r'SAIPE_income_n.csv', skiprows = 2)
income_n = income_n[['merge_level','Poverty Estimate, All Ages','Median Household Income']].rename(columns={'Poverty Estimate, All Ages':'poverty_allages_n','Median Household Income':'medianHHinc_n'})
accessandadoption_2016 = income_n.merge(accessandadoption_2016, how='inner', on='merge_level')

accessandadoption_2016.to_csv(os.path.join(workdir, r'accessandadoption2016_BPR_SAIPE.csv'))

import pandas as pd
import signal
import os
import numpy
import functools as func
os.chdir(r"/home/ec2-user/s3fs")
workdir = r'/home/ec2-user/s3fs'

file = pd.read_csv('file.csv', low_memory=False)

## Add BPR Report and Merge With File
bpr_c = pd.read_csv('bpr_2018_c.csv', low_memory=False)
bpr_s = pd.read_csv('bpr_2018_s.csv', low_memory=False)
bpr_n = pd.read_csv('bpr_2018_n.csv', low_memory=False)
file = file.merge(bpr_c, how='left', on='countyFIPS')
file = file.merge(bpr_s, how='left', on='stateFIPS')
file = file.merge(bpr_n, how='left', on='merge_level')

## Add SAIPE Report and Merge With File
income_c = pd.read_csv('SAIPE_income_c.csv', low_memory=False)
file = file.merge(income_c, how='left', on='countyFIPS')
income_s = pd.read_csv(r'SAIPE_income_s.csv', skiprows =3, low_memory=False)
income_s = income_s[['State FIPS Code','Poverty Estimate, All Ages','Median Household Income']].rename(columns={'State FIPS Code':'stateFIPS','Poverty Estimate, All Ages':'poverty_allages_s','Median Household Income':'medianHHinc_s'})
file = file.merge(income_s, how='left', on='stateFIPS')
income_n = pd.read_csv(r'SAIPE_income_n.csv', skiprows = 2, low_memory=False)
income_n = income_n[['merge_level','Poverty Estimate, All Ages','Median Household Income']].rename(columns={'Poverty Estimate, All Ages':'poverty_allages_n','Median Household Income':'medianHHinc_n'})
file = file.merge(income_n, how='inner', on='merge_level')

## Add RUCC Report and Merge With File
rucc = pd.read_csv('ERSruralurbancodes2013.csv', low_memory = False, encoding='latin=1')
rucc = rucc[['FIPS','Population_2010','RUCC_2013','Description','RUCC_metrononmetro']]
rucc = rucc.rename(columns={'FIPS':'countyFIPS','Population_2010':'RUCC_Pop_2010','Description':'RUCC_Description'})
file = file.merge(rucc, how='left', on='countyFIPS')

## Add OMB Report and Merge With File
OMB = pd.read_csv('OMBRuralDef.csv', encoding ='latin-1')
OMB = OMB[['Metropolitan/Micropolitan Statistical Area','Central/Outlying County','FIPS State Code','FIPS County Code']]
OMB['FIPS County Code'] = OMB['FIPS County Code'].astype(int)
OMB['State County Code'] = OMB['FIPS County Code'].astype(int)
OMB['FIPS County Code'] = OMB['FIPS County Code'].apply(lambda x: '{0:0>3}'.format(x))
OMB['countyFIPS'] = OMB['FIPS State Code'].astype(str) + OMB['FIPS County Code'].astype(str)
OMB['countyFIPS'] = OMB['countyFIPS'].astype(int)
OMB = OMB.rename(columns={'Metropolitan/Micropolitan Statistical Area':'OMB_metromicro_sa','Central/Outlying County':'OMB_countytype'})
OMB = OMB[['countyFIPS', 'OMB_metromicro_sa', 'OMB_countytype']]
file = file.merge(OMB, how='left', on='countyFIPS')
file.to_csv(os.path.join('file2.csv'))

## Add ERS Report and Merge With File
ERS = pd.read_csv('ERSCountyTypology2015.csv', skiprows=3, low_memory = False, encoding='latin=1')
ERS = ERS[['FIPStxt','Metro-nonmetro status, 2013 0=Nonmetro 1=Metro','Non-Overlapping Economic Types: Type_2015_Update','Farming_2015_Update (allows overlap, 1=yes)','Mining_2015-Update (allows overlap, 1=yes)','Manufacturing_2015_Update (allows overlap, 1=yes)','Government_2015_Update (allows overlap, 1=yes)','Recreation_2015_Update (allows overlap, 1=yes)','Nonspecialized_2015_Update (allows overlap, 1=yes)','Low_Education_2015_Update','Low_Employment_Cnty_2008_2012_25_64','Pop_Loss_2010','Retirement_Dest_2015_Update','Persistent_Poverty_2013','Persistent_Related_Child_Poverty_2013']]
ERS = ERS.rename(columns={'FIPStxt':'countyFIPS','Metro-nonmetro status, 2013 0=Nonmetro 1=Metro':'ERS_Metro_nonmetro','Non-Overlapping Economic Types: Type_2015_Update':'ERS_economictype','Farming_2015_Update (allows overlap, 1=yes)':'ERS_Farming','Mining_2015-Update (allows overlap, 1=yes)':'ERS_Mining','Manufacturing_2015_Update (allows overlap, 1=yes)':'ERS_Manufacturing','Government_2015_Update (allows overlap, 1=yes)':'ERS_Government','Recreation_2015_Update (allows overlap, 1=yes)':'ERS_Recreation','Nonspecialized_2015_Update (allows overlap, 1=yes)':'ERS_Nonspecialized','Low_Education_2015_Update':'ERS_Low_Education','Low_Employment_Cnty_2008_2012_25_64':'ERS_Low_Emp_Cnty_08_12','Pop_Loss_2010':'ERS_Pop_Loss_2010','Retirement_Dest_2015_Update':'ERS_Retirement_Dest','Persistent_Poverty_2013':'ERS_Pers_Pov','Persistent_Related_Child_Poverty_2013':'ERS_Pers_Rel_Child_Pov'})
file = file.merge(ERS, how='left', on='countyFIPS')

## ADD NCHS Report and Merge With File
nchs = pd.read_csv('NCHS_ruralcategories.csv', low_memory = False, encoding='latin=1')
nchs = nchs[['FIPS code','2013 code']]
nchs = nchs.rename(columns = {'FIPS code':'countyFIPS','2013 code':'NCHS_urbanruralcode'})
file = file.merge(nchs, how='left', on='countyFIPS')
file['NCHS_rural'] = ''

## Add CHR Report and Merge With File
Ranked_Measure_Data = pd.read_csv(r'CHR_RankedMeasureData.csv', skiprows =1, low_memory = False, encoding='latin=1')
CHR2017_c = pd.read_csv(r'CHR_AdditionalMeasureData.csv', skiprows=1, low_memory = False, encoding='latin=1')
CHR2017_c = CHR2017_c.merge(Ranked_Measure_Data, how='left', on='FIPS')
CHR2017_c = CHR2017_c.rename(columns={'Population_x':'Population'})
CHR2017_c = CHR2017_c.rename(columns={'% Diabetic':'DiabetesPCT_c','Other PCP Rate':'OtherPCPs_c','Household Income':'medianHHIncomeCHR_c','Population':'Population_c','% 65 and over':'pop65plusPCT_c','% Female':'popFemalePCT_c','% Rural':'popRuralPCT_c','# Rural':'PopRural_c','# Deaths':'prematureDeaths_c','% Fair/Poor':'FairPoorHealthPCT_c','Physically Unhealthy Days':'unhealthydays_c','% Smokers':'smokersPCT_c','% Obese':'obesePCT_c','% Physically Inactive':'physInactivePCT_c','% Excessive Drinking':'excessDrinkingPCT_c','# Primary Care Physicians':'PCPNum_c','PCP Ratio':'PopToPCPRatio_c','PCP Rate':'PCPper100000pop_c','# Dentists':'NumDentists_c','Dentist Ratio':'PopToDentistsRatio_c','Dentist Rate':'Dentistsper100000pop_c','# Mental Health Providers':'NumMHP_c','MHP Ratio':'PopToMHPRatio_c','MHP Rate':'MPHper100000pop_c','Preventable Hosp. Rate':'prevhosp_c','% Some College':'pctcollege_c','% Unemployed':'pctunemp_c','# Injury Deaths':'injurydeathrate_c','% Severe Housing Problems':'severehousingPCT_c','FIPS':'countyFIPS','State_x':'stateFIPS'})
CHR2017_c['PopFemale_c'] = CHR2017_c['Population_c']/(CHR2017_c['popFemalePCT_c']/100)*100
CHR2017_c['popMalePCT_c'] = 100-CHR2017_c['popFemalePCT_c']
CHR2017_c['PopMale_c'] = CHR2017_c['Population_c']/(CHR2017_c['popMalePCT_c']/100)*100
CHR2017_c = CHR2017_c[['countyFIPS','stateFIPS','DiabetesPCT_c','OtherPCPs_c','medianHHIncomeCHR_c','Population_c','pop65plusPCT_c','popFemalePCT_c','PopFemale_c','popMalePCT_c','PopMale_c','popRuralPCT_c','PopRural_c','prematureDeaths_c','FairPoorHealthPCT_c','unhealthydays_c','smokersPCT_c','obesePCT_c','physInactivePCT_c','excessDrinkingPCT_c','PCPNum_c','PopToPCPRatio_c','PCPper100000pop_c','NumDentists_c','PopToDentistsRatio_c','Dentistsper100000pop_c','NumMHP_c','PopToMHPRatio_c','MPHper100000pop_c','prevhosp_c','pctcollege_c','pctunemp_c','injurydeathrate_c','severehousingPCT_c']]
CHR2017_s = CHR2017_c.groupby(['stateFIPS'])
file = file.merge(CHR2017_c, how='left', on='countyFIPS')

## Add Housing Units and Get county_fips_population_housing_u
data = pd.read_csv('gl_data.csv')
file = file.merge(data, how='left', on='countyFIPS')
file['county_fips_population_housing_u'] = file['countyFIPS'].astype(str)+';'+file['hu2016'].astype(str)

## Add County FIPS Code
income_c = pd.read_csv(r'SAIPE_income_c_For_County_FIPS_Code.csv', skiprows =3)
income_c = income_c[['State FIPS Code','County FIPS Code','Postal Code']]
income_c['County FIPS Code2'] = income_c['County FIPS Code']
income_c['County FIPS Code'] = income_c['County FIPS Code'].apply(lambda x: '{0:0>3}'.format(x))
income_c['countyFIPS'] = income_c['State FIPS Code'].astype(str)+income_c['County FIPS Code'].astype(str)
income_c['countyFIPS'] = income_c['countyFIPS'].astype(int)
file = file.merge(income_c, how='left',on='countyFIPS')

## Add rural values
bb_access_c_r = pd.read_csv('bb_access_c_r.csv')
bb_access_s_r = pd.read_csv('bb_access_s_r.csv')
bb_access_n_r = pd.read_csv('bb_access_n_r.csv')

pop_c_r = pd.read_csv('rural_county_pop.csv')
pop_s_r = pd.read_csv('rural_state_pop.csv')
pop_n_r = pd.read_csv('rural_national_pop.csv')

file = file.rename(columns={'stateFIPS_x':'stateFIPS'})
file = file.merge(bb_access_c_r, how='left',on='countyFIPS')
file = file.merge(bb_access_s_r, how='left',on='stateFIPS')
file = file.merge(bb_access_n_r, how='left',on='merge_level')
file = file.merge(pop_c_r, how='left',on='countyFIPS')
file = file.merge(pop_s_r, how='left',on='stateFIPS')
file = file.merge(pop_n_r, how='left',on='merge_level')

## Add County Value with Only County Name and the the Word County
file['county'] = file['COUNTYNAME_x'].str.replace('County','')

## Add Dentists Ratio
dentists = pd.read_csv('dentists.csv')
file = file.merge(dentists, how='left', on='countyFIPS')

## Change Whole Numbers to 0 - 1 Scale
file['pctDS0_hi_c'] = file['pctDS0_hi_c']/100
file['pctDS0_hi_s'] = file['pctDS0_hi_s']/100
file['pctDS0_hi_n'] = file['pctDS0_hi_n']/100
file['pctDSGt0kAndLt1000k_hi_c'] = file['pctDSGt0kAndLt1000k_hi_c']/100
file['pctDSGt0kAndLt1000k_hi_n'] = file['pctDSGt0kAndLt1000k_hi_n']/100
file['pctDSGt1000kAndLt3000k_hi_c'] = file['pctDSGt1000kAndLt3000k_hi_c']/100
file['pctDSGt1000kAndLt3000k_hi_s'] = file['pctDSGt1000kAndLt3000k_hi_s'] /100
file['pctDSGt1000kAndLt3000k_hi_n'] = file['pctDSGt1000kAndLt3000k_hi_n']/100
file['pctDSGt3000kAndLt4000k_hi_c'] = file['pctDSGt3000kAndLt4000k_hi_c']/100
file['pctDSGt3000kAndLt4000k_hi_s'] = file['pctDSGt3000kAndLt4000k_hi_s']/100
file['pctDSGt3000kAndLt4000k_hi_n'] = file['pctDSGt3000kAndLt4000k_hi_n'] /100
file['pctDSGt4000kAndLt6000k_hi_c'] = file['pctDSGt4000kAndLt6000k_hi_c']/100
file['pctDSGt4000kAndLt6000k_hi_s'] = file['pctDSGt4000kAndLt6000k_hi_s']/100
file['pctDSGt4000kAndLt6000k_hi_n'] = file['pctDSGt4000kAndLt6000k_hi_n']/100
file['pctDSGt6000kAndLt10000k_hi_c'] = file['pctDSGt6000kAndLt10000k_hi_c']/100
file['pctDSGt6000kAndLt10000k_hi_s'] = file['pctDSGt6000kAndLt10000k_hi_s']/100
file['pctDSGt6000kAndLt10000k_hi_n'] = file['pctDSGt6000kAndLt10000k_hi_n']/100
file['pctDSGt10000kAndLt15000k_hi_c'] = file['pctDSGt10000kAndLt15000k_hi_c']/100
file['pctDSGt10000kAndLt15000k_hi_s'] = file['pctDSGt10000kAndLt15000k_hi_s']/100
file['pctDSGt10000kAndLt15000k_hi_n'] = file['pctDSGt10000kAndLt15000k_hi_n']/100
file['pctDSGt15000kAndLt25000k_hi_c'] = file['pctDSGt15000kAndLt25000k_hi_c']/100
file['pctDSGt15000kAndLt25000k_hi_s'] = file['pctDSGt15000kAndLt25000k_hi_s']/100
file['pctDSGt15000kAndLt25000k_hi_n'] = file['pctDSGt15000kAndLt25000k_hi_n']/100
file['pctDSGt25000kAndLt50000k_hi_c'] = file['pctDSGt25000kAndLt50000k_hi_c']/100
file['pctDSGt25000kAndLt50000k_hi_s'] = file['pctDSGt25000kAndLt50000k_hi_s']/100
file['pctDSGt25000kAndLt50000k_hi_n'] = file['pctDSGt25000kAndLt50000k_hi_n']/100
file['pctDSGt50000kAndLt100000k_hi_c'] = file['pctDSGt50000kAndLt100000k_hi_c']/100
file['pctDSGt50000kAndLt100000k_hi_s'] = file['pctDSGt50000kAndLt100000k_hi_s']/100
file['pctDSGt50000kAndLt100000k_hi_n'] = file['pctDSGt50000kAndLt100000k_hi_n']/100
file['pctDSGt100000kAndLt1Gig_hi_c'] = file['pctDSGt100000kAndLt1Gig_hi_c']/100
file['pctDSGt100000kAndLt1Gig_hi_s'] = file['pctDSGt100000kAndLt1Gig_hi_s']/100
file['pctDSGt100000kAndLt1Gig_hi_n'] = file['pctDSGt100000kAndLt1Gig_hi_n']/100
file['pctDSGt1Gig_hi_c'] = file['pctDSGt1Gig_hi_c']/100
file['pctDSGt1Gig_hi_s'] = file['pctDSGt1Gig_hi_s']/100
file['pctDSGt1Gig_hi_n'] = file['pctDSGt1Gig_hi_n']/100
file['pctUS0_hi_c'] = file['pctUS0_hi_c']/100
file['pctUS0_hi_s'] = file['pctUS0_hi_s']/100
file['pctUS0_hi_n'] = file['pctUS0_hi_n']/100
file['pctUSGt0kAndLt1000k_hi_c'] = file['pctUSGt0kAndLt1000k_hi_c']/100
file['pctUSGt0kAndLt1000k_hi_n'] = file['pctUSGt0kAndLt1000k_hi_n']/100
file['pctUSGt1000kAndLt3000k_hi_c'] = file['pctUSGt1000kAndLt3000k_hi_c']/100
file['pctUSGt1000kAndLt3000k_hi_s'] = file['pctUSGt1000kAndLt3000k_hi_s'] /100
file['pctUSGt1000kAndLt3000k_hi_n'] = file['pctUSGt1000kAndLt3000k_hi_n']/100
file['pctUSGt3000kAndLt4000k_hi_c'] = file['pctUSGt3000kAndLt4000k_hi_c']/100
file['pctUSGt3000kAndLt4000k_hi_s'] = file['pctUSGt3000kAndLt4000k_hi_s']/100
file['pctUSGt3000kAndLt4000k_hi_n'] = file['pctUSGt3000kAndLt4000k_hi_n'] /100
file['pctUSGt4000kAndLt6000k_hi_c'] = file['pctUSGt4000kAndLt6000k_hi_c']/100
file['pctUSGt4000kAndLt6000k_hi_s'] = file['pctUSGt4000kAndLt6000k_hi_s']/100
file['pctUSGt4000kAndLt6000k_hi_n'] = file['pctUSGt4000kAndLt6000k_hi_n']/100
file['pctUSGt6000kAndLt10000k_hi_c'] = file['pctUSGt6000kAndLt10000k_hi_c']/100
file['pctUSGt6000kAndLt10000k_hi_s'] = file['pctUSGt6000kAndLt10000k_hi_s']/100
file['pctUSGt6000kAndLt10000k_hi_n'] = file['pctUSGt6000kAndLt10000k_hi_n']/100
file['pctUSGt10000kAndLt15000k_hi_c'] = file['pctUSGt10000kAndLt15000k_hi_c']/100
file['pctUSGt10000kAndLt15000k_hi_s'] = file['pctUSGt10000kAndLt15000k_hi_s']/100
file['pctUSGt10000kAndLt15000k_hi_n'] = file['pctUSGt10000kAndLt15000k_hi_n']/100
file['pctUSGt15000kAndLt25000k_hi_c'] = file['pctUSGt15000kAndLt25000k_hi_c']/100
file['pctUSGt15000kAndLt25000k_hi_s'] = file['pctUSGt15000kAndLt25000k_hi_s']/100
file['pctUSGt15000kAndLt25000k_hi_n'] = file['pctUSGt15000kAndLt25000k_hi_n']/100
file['pctUSGt25000kAndLt50000k_hi_c'] = file['pctUSGt25000kAndLt50000k_hi_c']/100
file['pctUSGt25000kAndLt50000k_hi_s'] = file['pctUSGt25000kAndLt50000k_hi_s']/100
file['pctUSGt25000kAndLt50000k_hi_n'] = file['pctUSGt25000kAndLt50000k_hi_n']/100
file['pctUSGt50000kAndLt100000k_hi_c'] = file['pctUSGt50000kAndLt100000k_hi_c']/100
file['pctUSGt50000kAndLt100000k_hi_s'] = file['pctUSGt50000kAndLt100000k_hi_s']/100
file['pctUSGt50000kAndLt100000k_hi_n'] = file['pctUSGt50000kAndLt100000k_hi_n']/100
file['pctUSGt100000kAndLt1Gig_hi_c'] = file['pctUSGt100000kAndLt1Gig_hi_c']/100
file['pctUSGt100000kAndLt1Gig_hi_s'] = file['pctUSGt100000kAndLt1Gig_hi_s']/100
file['pctUSGt100000kAndLt1Gig_hi_n'] = file['pctUSGt100000kAndLt1Gig_hi_n']/100
file['pctUSGt1Gig_hi_c'] = file['pctUSGt1Gig_hi_c']/100
file['pctUSGt1Gig_hi_s'] = file['pctUSGt1Gig_hi_s']/100
file['pctUSGt1Gig_hi_n'] = file['pctUSGt1Gig_hi_n']/100

file['mcds_prop_c'] = file['mcds_prop_c']/100
file['mcds_prop_s'] = file['mcds_prop_s']/100
file['mcds_prop_n'] = file['mcds_prop_n']/100
file['mcus_prop_c'] = file['mcus_prop_c']/100
file['mcus_prop_s'] = file['mcus_prop_s']/100
file['mcus_prop_n'] = file['mcus_prop_n']/100

file['sum_prov_cnty_c_0'] = file['sum_prov_cnty_c_0']/100
file['sum_prov_cnty_c_1'] = file['sum_prov_cnty_c_1']/100
file['sum_prov_cnty_c_2'] = file['sum_prov_cnty_c_2']/100
file['sum_prov_cnty_c_3'] = file['sum_prov_cnty_c_3']/100
file['sum_prov_cnty_c_4'] = file['sum_prov_cnty_c_4']/100
file['sum_prov_cnty_c_5'] = file['sum_prov_cnty_c_5']/100
file['sum_prov_cnty_c_6'] = file['sum_prov_cnty_c_6']/100
file['sum_prov_cnty_c_7'] = file['sum_prov_cnty_c_7']/100
file['sum_prov_cnty_greq_c_8'] = file['sum_prov_cnty_greq_c_8']/100
file['sum_prov_cnty_b_0'] = file['sum_prov_cnty_b_0']/100
file['sum_prov_cnty_b_1'] = file['sum_prov_cnty_b_1']/100
file['sum_prov_cnty_b_2'] = file['sum_prov_cnty_b_2']/100
file['sum_prov_cnty_b_3'] = file['sum_prov_cnty_b_3']/100
file['sum_prov_cnty_b_4'] = file['sum_prov_cnty_b_4']/100
file['sum_prov_cnty_b_5'] = file['sum_prov_cnty_b_5']/100
file['sum_prov_cnty_b_6'] = file['sum_prov_cnty_b_6']/100
file['sum_prov_cnty_b_7'] = file['sum_prov_cnty_b_7']/100
file['sum_prov_cnty_greq_b_8'] = file['sum_prov_cnty_greq_b_8']/100
file['sum_prov_cnty_a_0'] = file['sum_prov_cnty_a_0']/100
file['sum_prov_cnty_a_1'] = file['sum_prov_cnty_a_1']/100
file['sum_prov_cnty_a_2'] = file['sum_prov_cnty_a_2']/100
file['sum_prov_cnty_a_3'] = file['sum_prov_cnty_a_3']/100
file['sum_prov_cnty_a_4'] = file['sum_prov_cnty_a_4']/100
file['sum_prov_cnty_a_5'] = file['sum_prov_cnty_a_5']/100
file['sum_prov_cnty_a_6'] = file['sum_prov_cnty_a_6']/100
file['sum_prov_cnty_a_7'] = file['sum_prov_cnty_a_7']/100
file['sum_prov_cnty_greq_a_8'] = file['sum_prov_cnty_greq_a_8']/100

file['sum_prov_state_c_0'] = file['sum_prov_state_c_0']/100
file['sum_prov_state_c_1'] = file['sum_prov_state_c_1']/100
file['sum_prov_state_c_2'] = file['sum_prov_state_c_2']/100
file['sum_prov_state_c_3'] = file['sum_prov_state_c_3']/100
file['sum_prov_state_c_4'] = file['sum_prov_state_c_4']/100
file['sum_prov_state_c_5'] = file['sum_prov_state_c_5']/100
file['sum_prov_state_c_6'] = file['sum_prov_state_c_6']/100
file['sum_prov_state_c_7'] = file['sum_prov_state_c_7']/100
file['sum_prov_state_greq_c_8'] = file['sum_prov_state_greq_c_8']/100
file['sum_prov_state_b_0'] = file['sum_prov_state_b_0']/100
file['sum_prov_state_b_1'] = file['sum_prov_state_b_1']/100
file['sum_prov_state_b_2'] = file['sum_prov_state_b_2']/100
file['sum_prov_state_b_3'] = file['sum_prov_state_b_3']/100
file['sum_prov_state_b_4'] = file['sum_prov_state_b_4']/100
file['sum_prov_state_b_5'] = file['sum_prov_state_b_5']/100
file['sum_prov_state_b_6'] = file['sum_prov_state_b_6']/100
file['sum_prov_state_b_7'] = file['sum_prov_state_b_7']/100
file['sum_prov_state_greq_b_8'] = file['sum_prov_state_greq_b_8']/100
file['sum_prov_state_a_0'] = file['sum_prov_state_a_0']/100
file['sum_prov_state_a_1'] = file['sum_prov_state_a_1']/100
file['sum_prov_state_a_2'] = file['sum_prov_state_a_2']/100
file['sum_prov_state_a_3'] = file['sum_prov_state_a_3']/100
file['sum_prov_state_a_4'] = file['sum_prov_state_a_4']/100
file['sum_prov_state_a_5'] = file['sum_prov_state_a_5']/100
file['sum_prov_state_a_6'] = file['sum_prov_state_a_6']/100
file['sum_prov_state_a_7'] = file['sum_prov_state_a_7']/100
file['sum_prov_state_greq_a_8'] = file['sum_prov_state_greq_a_8']/100

file['sum_prov_nat_c_0'] = file['sum_prov_nat_c_0']/100
file['sum_prov_nat_c_1'] = file['sum_prov_nat_c_1']/100
file['sum_prov_nat_c_2'] = file['sum_prov_nat_c_2']/100
file['sum_prov_nat_c_3'] = file['sum_prov_nat_c_3']/100
file['sum_prov_nat_c_4'] = file['sum_prov_nat_c_4']/100
file['sum_prov_nat_c_5'] = file['sum_prov_nat_c_5']/100
file['sum_prov_nat_c_6'] = file['sum_prov_nat_c_6']/100
file['sum_prov_nat_c_7'] = file['sum_prov_nat_c_7']/100
file['sum_prov_nat_greq_c_8'] = file['sum_prov_nat_greq_c_8']/100
file['sum_prov_nat_b_0'] = file['sum_prov_nat_b_0']/100
file['sum_prov_nat_b_1'] = file['sum_prov_nat_b_1']/100
file['sum_prov_nat_b_2'] = file['sum_prov_nat_b_2']/100
file['sum_prov_nat_b_3'] = file['sum_prov_nat_b_3']/100
file['sum_prov_nat_b_4'] = file['sum_prov_nat_b_4']/100
file['sum_prov_nat_b_5'] = file['sum_prov_nat_b_5']/100
file['sum_prov_nat_b_6'] = file['sum_prov_nat_b_6']/100
file['sum_prov_nat_b_7'] = file['sum_prov_nat_b_7']/100
file['sum_prov_nat_greq_b_8'] = file['sum_prov_nat_greq_b_8']/100
file['sum_prov_nat_a_0'] = file['sum_prov_nat_a_0']/100
file['sum_prov_nat_a_1'] = file['sum_prov_nat_a_1']/100
file['sum_prov_nat_a_2'] = file['sum_prov_nat_a_2']/100
file['sum_prov_nat_a_3'] = file['sum_prov_nat_a_3']/100
file['sum_prov_nat_a_4'] = file['sum_prov_nat_a_4']/100
file['sum_prov_nat_a_5'] = file['sum_prov_nat_a_5']/100
file['sum_prov_nat_a_6'] = file['sum_prov_nat_a_6']/100
file['sum_prov_nat_a_7'] = file['sum_prov_nat_a_7']/100
file['sum_prov_nat_greq_a_8'] = file['sum_prov_nat_greq_a_8']/100

file['cumm_prov_cnty_c_0'] = file['cumm_prov_cnty_c_0']/100
file['cumm_prov_cnty_c_1'] = file['cumm_prov_cnty_c_1']/100
file['cumm_prov_cnty_c_2'] = file['cumm_prov_cnty_c_2']/100
file['cumm_prov_cnty_c_3'] = file['cumm_prov_cnty_c_3']/100
file['cumm_prov_cnty_c_4'] = file['cumm_prov_cnty_c_4']/100
file['cumm_prov_cnty_c_5'] = file['cumm_prov_cnty_c_5']/100
file['cumm_prov_cnty_c_6'] = file['cumm_prov_cnty_c_6']/100
file['cumm_prov_cnty_c_7'] = file['cumm_prov_cnty_c_7']/100
file['cumm_prov_cnty_c_8'] = file['cumm_prov_cnty_c_8']/100

file['cumm_prov_cnty_b_0'] = file['cumm_prov_cnty_b_0']/100
file['cumm_prov_cnty_b_1'] = file['cumm_prov_cnty_b_1']/100
file['cumm_prov_cnty_b_2'] = file['cumm_prov_cnty_b_2']/100
file['cumm_prov_cnty_b_3'] = file['cumm_prov_cnty_b_3']/100
file['cumm_prov_cnty_b_4'] = file['cumm_prov_cnty_b_4']/100
file['cumm_prov_cnty_b_5'] = file['cumm_prov_cnty_b_5']/100
file['cumm_prov_cnty_b_6'] = file['cumm_prov_cnty_b_6']/100
file['cumm_prov_cnty_b_7'] = file['cumm_prov_cnty_b_7']/100
file['cumm_prov_cnty_b_8'] = file['cumm_prov_cnty_b_8']/100

file['cumm_prov_cnty_a_0'] = file['cumm_prov_cnty_a_0']/100
file['cumm_prov_cnty_a_1'] = file['cumm_prov_cnty_a_1']/100
file['cumm_prov_cnty_a_2'] = file['cumm_prov_cnty_a_2']/100
file['cumm_prov_cnty_a_3'] = file['cumm_prov_cnty_a_3']/100
file['cumm_prov_cnty_a_4'] = file['cumm_prov_cnty_a_4']/100
file['cumm_prov_cnty_a_5'] = file['cumm_prov_cnty_a_5']/100
file['cumm_prov_cnty_a_6'] = file['cumm_prov_cnty_a_6']/100
file['cumm_prov_cnty_a_7'] = file['cumm_prov_cnty_a_7']/100
file['cumm_prov_cnty_a_8'] = file['cumm_prov_cnty_a_8']/100

file['cumm_prov_state_c_0'] = file['cumm_prov_state_c_0']/100
file['cumm_prov_state_c_1'] = file['cumm_prov_state_c_1']/100
file['cumm_prov_state_c_2'] = file['cumm_prov_state_c_2']/100
file['cumm_prov_state_c_3'] = file['cumm_prov_state_c_3']/100
file['cumm_prov_state_c_4'] = file['cumm_prov_state_c_4']/100
file['cumm_prov_state_c_5'] = file['cumm_prov_state_c_5']/100
file['cumm_prov_state_c_6'] = file['cumm_prov_state_c_6']/100
file['cumm_prov_state_c_7'] = file['cumm_prov_state_c_7']/100
file['cumm_prov_state_c_8'] = file['cumm_prov_state_c_8']/100

file['cumm_prov_state_b_0'] = file['cumm_prov_state_b_0']/100
file['cumm_prov_state_b_1'] = file['cumm_prov_state_b_1']/100
file['cumm_prov_state_b_2'] = file['cumm_prov_state_b_2']/100
file['cumm_prov_state_b_3'] = file['cumm_prov_state_b_3']/100
file['cumm_prov_state_b_4'] = file['cumm_prov_state_b_4']/100
file['cumm_prov_state_b_5'] = file['cumm_prov_state_b_5']/100
file['cumm_prov_state_b_6'] = file['cumm_prov_state_b_6']/100
file['cumm_prov_state_b_7'] = file['cumm_prov_state_b_7']/100
file['cumm_prov_state_b_8'] = file['cumm_prov_state_b_8']/100

file['cumm_prov_state_a_0'] = file['cumm_prov_state_a_0']/100
file['cumm_prov_state_a_1'] = file['cumm_prov_state_a_1']/100
file['cumm_prov_state_a_2'] = file['cumm_prov_state_a_2']/100
file['cumm_prov_state_a_3'] = file['cumm_prov_state_a_3']/100
file['cumm_prov_state_a_4'] = file['cumm_prov_state_a_4']/100
file['cumm_prov_state_a_5'] = file['cumm_prov_state_a_5']/100
file['cumm_prov_state_a_6'] = file['cumm_prov_state_a_6']/100
file['cumm_prov_state_a_7'] = file['cumm_prov_state_a_7']/100
file['cumm_prov_state_a_8'] = file['cumm_prov_state_a_8']/100

file['cumm_prov_nat_c_0'] = file['cumm_prov_nat_c_0']/100
file['cumm_prov_nat_c_1'] = file['cumm_prov_nat_c_1']/100
file['cumm_prov_nat_c_2'] = file['cumm_prov_nat_c_2']/100
file['cumm_prov_nat_c_3'] = file['cumm_prov_nat_c_3']/100
file['cumm_prov_nat_c_4'] = file['cumm_prov_nat_c_4']/100
file['cumm_prov_nat_c_5'] = file['cumm_prov_nat_c_5']/100
file['cumm_prov_nat_c_6'] = file['cumm_prov_nat_c_6']/100
file['cumm_prov_nat_c_7'] = file['cumm_prov_nat_c_7']/100
file['cumm_prov_nat_c_8'] = file['cumm_prov_nat_c_8']/100

file['cumm_prov_nat_b_0'] = file['cumm_prov_nat_b_0']/100
file['cumm_prov_nat_b_1'] = file['cumm_prov_nat_b_1']/100
file['cumm_prov_nat_b_2'] = file['cumm_prov_nat_b_2']/100
file['cumm_prov_nat_b_3'] = file['cumm_prov_nat_b_3']/100
file['cumm_prov_nat_b_4'] = file['cumm_prov_nat_b_4']/100
file['cumm_prov_nat_b_5'] = file['cumm_prov_nat_b_5']/100
file['cumm_prov_nat_b_6'] = file['cumm_prov_nat_b_6']/100
file['cumm_prov_nat_b_7'] = file['cumm_prov_nat_b_7']/100
file['cumm_prov_nat_b_8'] = file['cumm_prov_nat_b_8']/100

file['cumm_prov_nat_a_0'] = file['cumm_prov_nat_a_0']/100
file['cumm_prov_nat_a_1'] = file['cumm_prov_nat_a_1']/100
file['cumm_prov_nat_a_2'] = file['cumm_prov_nat_a_2']/100
file['cumm_prov_nat_a_3'] = file['cumm_prov_nat_a_3']/100
file['cumm_prov_nat_a_4'] = file['cumm_prov_nat_a_4']/100
file['cumm_prov_nat_a_5'] = file['cumm_prov_nat_a_5']/100
file['cumm_prov_nat_a_6'] = file['cumm_prov_nat_a_6']/100
file['cumm_prov_nat_a_7'] = file['cumm_prov_nat_a_7']/100
file['cumm_prov_nat_a_8'] = file['cumm_prov_nat_a_8']/100

file['cumm_prov_cnty_c_50th'] = file['cumm_prov_cnty_c_50th']/100
file['cumm_prov_cnty_b_50th'] = file['cumm_prov_cnty_b_50th']/100	
file['cumm_prov_cnty_a_50th'] = file['cumm_prov_cnty_a_50th']/100

file['cumm_prov_state_c_50th'] = file['cumm_prov_state_c_50th']/100	
file['cumm_prov_state_b_50th'] = file['cumm_prov_state_b_50th']/100		
file['cumm_prov_state_a_50th'] = file['cumm_prov_state_a_50th']/100

file['cumm_prov_nat_c_50th'] = file['cumm_prov_nat_c_50th']/100	
file['cumm_prov_nat_b_50th'] = file['cumm_prov_nat_b_50th']/100		
file['cumm_prov_nat_a_50th'] = file['cumm_prov_nat_a_50th']/100

file['dsgteq25_c'] = file['dsgteq25_c']/100
file['dsgteq25_s'] = file['dsgteq25_s']/100
file['dsgteq25_n'] = file['dsgteq25_n']/100
file['usgteq3_c'] = file['usgteq3_c']/100
file['usgteq3_s'] = file['usgteq3_s']/100
file['usgteq3_n'] = file['usgteq3_n']/100

ruralblocks = pd.read_csv('blocks_with_rural_designations.csv', dtype={'blocks_fips':'str'})
ruralblocks = ruralblocks[['block_fips','urban_rural','county_fips']]
ruralblocks['block_fips'] = ruralblocks['block_fips'].apply(lambda x: '{0:0>15}'.format(x))

allblocks = pd.read_csv('us2016.csv', dtype={'blocks_fips':'str'})
allblocks = allblocks[['block_fips','pop2016']]
allblocks['block_fips'] = allblocks['block_fips'].apply(lambda x: '{0:0>15}'.format(x))

ruralblocks = ruralblocks.merge(allblocks, how='inner', on='block_fips')

ruralblocks['stateFIPS'] = pd.to_numeric(ruralblocks['block_fips'].astype(str).str[:2])
ruralblocks['countyFIPS'] = pd.to_numeric(ruralblocks['block_fips'].astype(str).str[:5])
county_rural_pop = ruralblocks.groupby(['countyFIPS'], as_index = False)['pop2016'].sum()
state_rural_pop = ruralblocks.groupby(['stateFIPS'], as_index = False)['pop2016'].sum()

file = file.merge(county_rural_pop, how='left', on='countyFIPS')
file = file.merge(state_rural_pop, how='left', on='stateFIPS')

file.to_csv(os.path.join('file2.csv'))

