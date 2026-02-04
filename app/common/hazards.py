from enum import Enum


class HazardEnum(str, Enum):
    tavg = "TAVG"
    tmax = "TMAX"
    ndl0 = "NDL0"
    ntx35 = "NTx35"
    ntx40 = "NTx40"
    hsh_max = "HSH-max"
    ndws = "NDWS"
    ptot = "PTOT"
    thi_max = "THI-max"
