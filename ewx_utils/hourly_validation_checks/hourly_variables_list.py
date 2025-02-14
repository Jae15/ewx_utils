""" This script stores the mawndbhourly table variable list whereby various variables are stored in different categories such as those relating to temperature etc.
The variable categories are referenced by the script mawndbsrc.py and used in the data validation process.
"""

""" Relative humidity variables List - if the k - key - is in this list, treat the variable as 'relh_vars' """

relh_vars = [
    "relh",
    "relh_3m",
    "relh_3m_max",
    "relh_3m_min",
    "relh_45cm",
    "relh_45cm_max",
    "relh_45cm_min",
    "relh_max",
    "relh_min"
]

""" pcpn/rpet  hourly and daily have different validations (they are sums - you can have more pcpn in a day than an hour) """
""" Precipitation variables list - if k - key is in this list, treat the variable as 'pcpn_vars' """

pcpn_vars = ["pcpn"]

""" Evapotranspiration variables list - if k - key is in this list, treat the variable as 'rpet_vars' """

rpet_vars = ["rpet"]

""" Temperature variables List - if the k - key - is in this list, treat as 'temp_vars' """

temp_vars = [
    "atmp",
    "atmp_max",
    "atmp_min",
    "atmp_10cm",
    "atmp_10cm_max",
    "atmp_10cm_min",
    "atmp_10m",
    "atmp_10m_max",
    "atmp_10m_min",
    "atmp_20m",
    "atmp_20m_max",
    "atmp_20m_min",
    "atmp_3m",
    "atmp_3m_max",
    "atmp_3m_min",
    "atmp_45cm",
    "atmp_45cm_max",
    "atmp_45cm_min",
    "atmp06in",
    "atmp06in_max",
    "atmp06in_min",
    "ctmp00ft",
    "ctmp00ft_max",
    "ctmp00ft_min",
    "ctmp02ft",
    "ctmp02ft_max",
    "ctmp02ft_min",
    "polyatmp",
    "polyatmp_max",
    "polyatmp_min",
    "polystmp1",
    "polystmp1_max",
    "polystmp1_min",
    "polystmp2",
    "polystmp2_max",
    "polystmp2_min",
    "soil_100cm",
    "soil_100cm_max",
    "soil_100cm_min",
    "soil_20cm",
    "soil_20cm_max",
    "soil_20cm_min",
    "soil_50cm",
    "soil_50cm_max",
    "soil_50cm_min",
    "soil0",
    "soil0_max",
    "soil0_min",
    "soil1",
    "soil1_max",
    "soil1_min",
    "soil2",
    "soil2_max",
    "soil2_min",
    "soil3",
    "soil3_max",
    "soil3_min",
    "stmp06in",
    "stmp06in_max",
    "stmp06in_min",
    "stmp12in",
    "stmp12in_max",
    "stmp12in_min",
    "stmp16in",
    "stmp16in_max",
    "stmp16in_min",
    "stmp_05cm",
    "stmp_05cm_max",
    "stmp_05cm_min",
    "stmp_10cm",
    "stmp_10cm_max",
    "stmp_10cm_min",
    "stmp_20cm",
    "stmp_20cm_max",
    "stmp_20cm_min",
    "stmp_50cm",
    "stmp_50cm_max",
    "stmp_50cm_min",
    "stmp_100cm",
    "stmp_100cm_max",
    "stmp_100cm_min"
]


""" Windspeed variables list - if the k - key - is in this list, treat the variable as 'wspd_vars' """

wspd_vars = ["wspd", 
             "wspd_10m", 
             "wspd_20m", 
             "wspd_max", 
             "wspd_10m_max", 
             "wspd_20m_max"]

""" Leaf wetness variables List - if the k - key - is in this list, treat the variable as 'leafwt_vars' """

leafwt_vars = ["leaf0", 
               "leaf1", 
               "lws0_pwet", 
               "lws1_pwet"]

""" Net radiation variables List - if the k - key - is in this list, treat the variable as 'nrad-vars' """

nrad_vars = ["nrad", 
             "nrad_max", 
             "nrad_min"]

""" Solar radiation variables list - if the k - key - is in this list, treat the variable as 'srad_vars' """

srad_vars = ["srad"]

""" Wind direction variables list - if the k - key - is in this list, treat the varible as srad_vars """

wdir_vars = ["wdir", 
             "wdir_10m", 
             "wdir_20m"]

""" Voltage variables list - if the k - key - is in this list, treat the variable as volt_vars """

volt_vars = ["volt", 
             "volt_min"]

""" Soil Moisture variables list - if the k - key - is in this list, treat the variable as mstr_vars """

mstr_vars = [
    "mstr0",
    "mstr1",
    "mstr_90cm",
    "mstr_120cm",
    "smst_05cm",
    "smst_10cm",
    "smst_20cm",
    "smst_50cm" ,
    "smst_100cm"]

dwpt_vars =  [ "dwpt",
    "dwpt_min",
    "dwpt_max",
    "dwpt_3m",
    "dwpt_45cm",
    "dwpt_3m_min",
    "dwpt_3m_max",
    "dwpt_45cm_min",
    "dwpt_45cm_max"
]

vapr_vars = [
    "vapr",
    "vapr_min",
    "vapr_max",
    "vapr_3m",
    "vapr_3m_min",
    "vapr_45cm",
    "vapr_45cm_min",
    "vapr_45m_max"
]

sflux_vars = [
    "sflux",
    "sflux_min",
    "sflux_max"
]

wstdv_vars = [
    "wstdv",
    "wstdv_min",
    "wstdv_max",
    "wstdv_10m",
    "wstdv_20m"
]


