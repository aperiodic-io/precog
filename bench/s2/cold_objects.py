#!/usr/bin/env python3
"""Effective $/TB-month of object stores for sealed segments (reports/S2 cold storage.md):
128 MB objects, 14- and 60-day retention, each read once into another provider. No inputs."""
EUR=1.15            # assumed USD per EUR [E]
GIB=1e12/2**30      # GiB per decimal TB = 931.3
OBJ=1e12/128e6      # 128 MB objects per TB = 7812.5
# name: (storage $/TB-mo, min days, egress $/TB, retrieval $/TB, put $/M, get $/M, free-egress multiple of stored (None=none, inf=all), note)
P={
 'OVH 1-AZ Standard':      (0.0081103*GIB,0,0,0,0,0,'inf'),
 'OVH 1-AZ Infrequent Access':(0.0043581*GIB,30,0,0.00436*GIB,0,0,'inf'),
 'OVH 3-AZ Standard (>500 TiB tier)':(0.012337*GIB,0,0,0,0,0,'inf'),
 'OVH 3-AZ Infrequent Access':(0.0103514*GIB,30,0,0.00436*GIB,0,0,'inf'),
 'OVH 3-AZ Active Archive':(0.0053217*GIB,90,0,0.0212*GIB,0,0,'inf'),
 'Backblaze B2':           (6.95,0,10,0,0,0,3),
 'iDrive e2':              (6.00,0,10,0,0,0,3),
 'Hetzner Object Storage': (0.0087*730*EUR,0,1.0*EUR,0,0,0,None),
 'Storj (Standard)':       (7.00,30,7,0,0,0,None),
 'Synology C2 [3P]':       (7.99,0,0,0,0,0,'inf'),
 'Wasabi PAYG':            (7.99,90,0,0,0,0,1),
 'Impossible Cloud':       (7.99*EUR,0,0,0,0,0,'inf'),
 'Scaleway One Zone':      (0.000011*730*1000*EUR,0,10*EUR,0,0,0,None),
 'Scaleway Multi-AZ':      (0.000022*730*1000*EUR,0,10*EUR,0,0,0,None),
 'Contabo [3P]':           (9.96*EUR,0,0,0,0,0,'inf'),
 'Cloudflare R2 IA':       (10,30,0,10,9.0,0.90,'inf'),
 'Cloudflare R2 Standard': (15,0,0,0,4.5,0.36,'inf'),
 'Filebase':               (15,0,0,0,0,0,'inf'),
 'DigitalOcean Spaces Cold':(0.007*GIB,30,0.01*GIB,0.01*GIB,0,0,None),
 'DigitalOcean Spaces':    (0.02*GIB,0,0.01*GIB,0,0,0,None),
 'Vultr Standard':         (18,0,10,0,0,0,None),
 'Linode/Akamai':          (20,0,5,0,0,0,None),
 'UpCloud [3P]':           (20,0,0,0,0,0,'inf'),
 'Exoscale SOS':           (0.0000275*730*GIB*EUR,0,20*EUR,0,0,0,None),
 'AWS S3 Glacier IR':      (4,90,90,30,20,10,None),
 'AWS S3 One Zone-IA':     (10,30,90,10,10,1,None),
 'AWS S3 Standard-IA':     (12.5,30,90,10,10,1,None),
 'GCS Coldline (regional)':(0.004*GIB,90,0.12*GIB,0.02*GIB,20,10,None),
 'GCS Nearline (regional)':(0.010*GIB,30,0.12*GIB,0.01*GIB,10,1,None),
 'Azure Cold LRS':         (3.6,90,87,30,18,10,None),
 'Azure Cool LRS':         (15.2,30,87,10,10,1,None),
}
def eff(p,R):
    s,mn,eg,rt,put,get,free=p
    m=R/30.0                       # stored-months per TB written
    store=s*max(R,mn)/30.0
    reads_per_stored_month=1/m     # TB read per stored TB per month
    if free=='inf': egc=0
    elif free is None: egc=eg
    else: egc=eg if reads_per_stored_month>free else 0
    req=OBJ*(put+get)/1e6
    return (store+egc+rt+req)/m, store/m, (egc+rt)/m
for R in (14,60):
    rows=sorted(((eff(p,R),n) for n,p in P.items()))
    print(f'\n## R={R} days')
    for (t,s,r),n in rows: print(f'{n:36s} list {P[n][0]:6.2f}  eff {t:7.2f}  (storage {s:6.2f} read {r:6.2f})')

# ---- Part 2: colo TCO per 60-bay chassis
drive_tb=24; bays=60; raw=drive_tb*bays
def tco(drive_usd, chassis_usd, colo_usd_mo, afr=0.015, years=5, fill=0.85, k=8, m=3, rh_usd_hr=97):
    capex=bays*drive_usd+chassis_usd
    amort=capex/(years*12)
    repl=bays*afr*drive_usd/12 + bays*afr*0.5*rh_usd_hr/12
    mo=amort+repl+colo_usd_mo
    usable=raw*k/(k+m)*fill
    return mo, mo/raw, mo/usable
power_kw=0.75
colo={
 'Hetzner HEL colo (1/3 rack shared by 3, EUR0.22/kWh)': (100/3+0.22*power_kw*730)*EUR,
 'Hetzner NBG/FSN colo (1/3 rack /3, EUR0.33/kWh)':      (100/3+0.33*power_kw*730)*EUR,
 'xaweho Dusseldorf full rack /9 (EUR327.73/kW net)':    ((462.18+327.73*9*power_kw)/9)*EUR,
 'Voxility Templus AMS01 full rack 6 kW /8':             (1099.84+1622.88)/8,
 'Voxility DLR FRA8 full rack 6 kW /8':                  (2564.80+1835.68)/8,
 'CBRE wholesale $165-265/kW (midpoint $215) [3P]':      215*power_kw,
}
for scen,(d,c) in {'new chassis, $749 recert 24TB':(749,14000),'used chassis, $749 recert 24TB':(749,2500),'used chassis, 2025 drive price $299':(299,2500)}.items():
    print('\n###',scen)
    for n,cm in colo.items():
        mo,r,u=tco(d,c,cm); _,_,u63=tco(d,c,cm,k=6,m=3)
        print(f'{n:58s} colo ${cm:6.0f}/mo  total ${mo:6.0f}/mo  raw ${r:4.2f}  8+3 ${u:4.2f}  6+3 ${u63:4.2f}')
