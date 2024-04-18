#! /usr/bin/env python3

import sys
import os
import argparse
import csv
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random
import hashlib

csv.field_size_limit(sys.maxsize)

#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

    #----------------------------------------
    def map(self, raw_data, input_row_num = None):
        json_data = {}

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--mandatory attributes
        json_data['DATA_SOURCE'] = 'SAFEGRAPH'

        #--the record_id should be unique, remove this mapping if there is not one 
        json_data['RECORD_ID'] = raw_data['PLACEKEY']

        #--record type is not mandatory, but should be PERSON or ORGANIZATION
        json_data['RECORD_TYPE'] = 'ORGANIZATION'

        #--column mappings

        # columnName: PLACEKEY
        # 100.0 populated, 100.0 unique
        #      227-223@5x4-4pp-jgk (1)
        #      227-222@5x4-8cs-fpv (1)
        #      223-223@5wb-wmq-jqf (1)
        #      228-222@5x4-4b5-kvf (1)
        #      22j-223@5x4-2jf-h3q (1)
        json_data['PLACEKEY'] = raw_data['PLACEKEY']

        # columnName: PARENT_PLACEKEY
        # 11.13 populated, 18.23 unique
        #      224-2jn@5x4-4b5-skf (400)
        #      zzw-222@5x4-8fn-3t9 (276)
        #      zzy-22q@5x4-4b5-p5f (238)
        #      zzw-227@5x4-86x-cqz (238)
        #      22q-22c@5x4-4s5-ch5 (236)
        #json_data['PARENT_PLACEKEY'] = raw_data['PARENT_PLACEKEY']
        json_data['REL_ANCHOR_DOMAIN'] = 'PLACEKEY'
        json_data['REL_ANCHOR_KEY'] = raw_data['PLACEKEY']
        if raw_data['PARENT_PLACEKEY'] and raw_data['PARENT_PLACEKEY'] != raw_data['PLACEKEY']:
            json_data['REL_POINTER_DOMAIN'] = 'PLACEKEY'
            json_data['REL_POINTER_KEY'] = raw_data['PARENT_PLACEKEY']
            json_data['REL_POINTER_ROLE'] = 'PARENT'

        # columnName: SAFEGRAPH_BRAND_IDS
        # 16.52 populated, 4.15 unique
        #      SG_BRAND_b43fa926d19a5168 (2916)
        #      SG_BRAND_8ef71dae032dc45d25ebf2c5fee7f15b (1000)
        #      SG_BRAND_9ee39f394d21a7f4848ab78a78da00c3 (957)
        #      SG_BRAND_27d57a8a9c6b943f (856)
        #      SG_BRAND_21bc583634047f283e0ecd2291b60f31 (845)
        #json_data['SAFEGRAPH_BRAND_IDS'] = raw_data['SAFEGRAPH_BRAND_IDS']

        # columnName: LOCATION_NAME
        # 100.0 populated, 74.99 unique
        #      USPS Collection Point (2916)
        #      Redbox (1000)
        #      Western Union (957)
        #      CO-OP Network ATM (856)
        #      UPS Drop Box (845)
        json_data['LOCATION_NAME_ORG'] = raw_data['LOCATION_NAME']

        # columnName: BRANDS
        # 16.52 populated, 4.15 unique
        #      USPS Collection Point (2916)
        #      Redbox (1000)
        #      Western Union (957)
        #      CO-OP Network ATM (856)
        #      UPS Drop Box (845)
        json_data['BRANDS'] = raw_data['BRANDS']

        # columnName: STORE_ID
        # 15.82 populated, 85.48 unique
        #      13 (17)
        #      4 (15)
        #      6 (15)
        #      5 (15)
        #      14 (15)
        #json_data['STORE_ID'] = raw_data['STORE_ID']

        # columnName: TOP_CATEGORY
        # 99.96 populated, 0.07 unique
        #      Restaurants and Other Eating Places (26871)
        #      Urban Transit Systems (24548)
        #      Lessors of Real Estate (19617)
        #      Personal Care Services (15075)
        #      Offices of Physicians (12994)
        json_data['TOP_CATEGORY'] = raw_data['TOP_CATEGORY']

        # columnName: SUB_CATEGORY
        # 88.23 populated, 0.14 unique
        #      Bus and Other Motor Vehicle Transit Systems (24368)
        #      Lessors of Residential Buildings and Dwellings (17633)
        #      Full-Service Restaurants (15055)
        #      Offices of Physicians (except Mental Health Specialists) (12911)
        #      Hair, Nail, and Skin Care Services (9769)
        json_data['SUB_CATEGORY'] = raw_data['SUB_CATEGORY']

        # columnName: NAICS_CODE
        # 99.96 populated, 0.14 unique
        #      485113 (24368)
        #      531110 (17633)
        #      722511 (15055)
        #      621111 (12911)
        #      81211 (9769)
        json_data['NAICS_CODE'] = raw_data['NAICS_CODE']

        # columnName: LATITUDE
        # 100.0 populated, 85.48 unique
        #      47.609615 (19)
        #      47.609636 (18)
        #      47.609632 (14)
        #      47.609605 (14)
        #      47.609639 (14)
        json_data['BUSINESS_GEO_LATITUDE'] = raw_data['LATITUDE']

        # columnName: LONGITUDE
        # 100.0 populated, 83.61 unique
        #      -122.328012 (22)
        #      -122.328033 (19)
        #      -122.328032 (17)
        #      -122.32799 (16)
        #      -122.32803 (15)
        json_data['BUSINESS_GEO_LONGITUDE'] = raw_data['LONGITUDE']

        # NOTE: BUSINESS_ADDR_FULL MAPPED BELOW

        # columnName: STREET_ADDRESS
        # 100.0 populated, 66.16 unique
        #      1100 9th Ave (454)
        #      17801 International Blvd (163)
        #      1145 Broadway (116)
        #      3000 184th St SW (111)
        #      4502 S Steele St (104)
        #json_data['STREET_ADDRESS'] = raw_data['STREET_ADDRESS']

        # columnName: PRIMARY_NUMBER
        # 89.95 populated, 8.18 unique
        #      1100 (860)
        #      101 (802)
        #      400 (793)
        #      100 (773)
        #      500 (758)
        #json_data['PRIMARY_NUMBER'] = raw_data['PRIMARY_NUMBER']

        # columnName: STREET_PREDIRECTION
        # 36.76 populated, 0.01 unique
        #      NE (19553)
        #      S (18565)
        #      E (17637)
        #      N (16815)
        #      W (15397)
        #json_data['STREET_PREDIRECTION'] = raw_data['STREET_PREDIRECTION']

        # columnName: STREET_NAME
        # 99.68 populated, 5.03 unique
        #      Main (6276)
        #      1st (5555)
        #      Pacific (4736)
        #      4th (3393)
        #      5th (2927)
        #json_data['STREET_NAME'] = raw_data['STREET_NAME']

        # columnName: STREET_POSTDIRECTION
        # 31.8 populated, 0.01 unique
        #      NE (20302)
        #      S (15129)
        #      SW (12953)
        #      SE (12449)
        #      E (9126)
        #json_data['STREET_POSTDIRECTION'] = raw_data['STREET_POSTDIRECTION']

        # columnName: STREET_SUFFIX
        # 93.21 populated, 0.15 unique
        #      Ave (88685)
        #      St (81440)
        #      Rd (24302)
        #      Way (20938)
        #      Dr (12432)
        #json_data['STREET_SUFFIX'] = raw_data['STREET_SUFFIX']

        # columnName: CITY
        # 100.0 populated, 0.28 unique
        #      Seattle (43862)
        #      Tacoma (13184)
        #      Spokane (12965)
        #      Vancouver (12085)
        #      Bellevue (10166)
        #json_data['CITY'] = raw_data['CITY']

        # columnName: REGION
        # 100.0 populated, 0.0 unique
        #      WA (281628)
        #json_data['REGION'] = raw_data['REGION']

        # columnName: POSTAL_CODE
        # 100.0 populated, 0.25 unique
        #      98004 (4547)
        #      98101 (3722)
        #      98225 (3662)
        #      98052 (3414)
        #      98104 (3296)
        #json_data['POSTAL_CODE'] = raw_data['POSTAL_CODE']

        # columnName: OPEN_HOURS
        # 46.99 populated, 26.43 unique
        #json_data['OPEN_HOURS'] = raw_data['OPEN_HOURS']

        # columnName: CATEGORY_TAGS
        # 69.71 populated, 5.23 unique
        #      Bus Station,Buses (24368)
        #      Churches,Hindu Temple,Mosque,Sikh Temple,Synagogues (3817)
        #      Hair Salon (3096)
        #      Mail Boxes,Couriers,Packing Services,Packing Supplies (2919)
        #      Churches (2674)
        json_data['CATEGORY_TAGS'] = raw_data['CATEGORY_TAGS']

        # columnName: OPENED_ON
        # 1.7 populated, 1.0 unique
        #      2021-01 (331)
        #      2022-04 (311)
        #      2023-01 (234)
        #      2022-11 (224)
        #      2022-09 (191)
        json_data['OPENED_ON'] = raw_data['OPENED_ON']

        # columnName: CLOSED_ON
        # 13.23 populated, 0.12 unique
        #      2020-01 (8829)
        #      2021-10 (3873)
        #      2022-03 (3388)
        #      2022-12 (3363)
        #      2022-04 (2792)
        json_data['CLOSED_ON'] = raw_data['CLOSED_ON']

        # columnName: TRACKING_CLOSED_SINCE
        # 97.49 populated, 0.02 unique
        #      2019-07 (258205)
        #      2022-11 (3625)
        #      2021-06 (1913)
        #      2022-10 (1158)
        #      2021-03 (881)
        json_data['TRACKING_CLOSED_SINCE'] = raw_data['TRACKING_CLOSED_SINCE']

        # columnName: WEBSITES
        # 100.0 populated, 27.75 unique
        #      [] (134457)
        #      [
        #json_data['WEBSITES'] = raw_data['WEBSITES']

        # columnName: GEOMETRY_TYPE
        # 100.0 populated, 0.0 unique
        #      POLYGON (240618)
        #      POINT (41010)
        #json_data['GEOMETRY_TYPE'] = raw_data['GEOMETRY_TYPE']

        # columnName: POLYGON_WKT
        # 85.32 populated, 62.45 unique
        #      POLYGON ((-122.32800399309454 47.60978359301257, -122.3278253999499 47.60934425387672, -122.32798145899994 47.60928265100006, -122.32843595499997 47.60982366600007, -122.32830238899999 47.60987639000007, -122.32816953599996 47.609718247000046, -122.32800399309454 47.60978359301257)) (426)
        #      POLYGON ((-122.25936789299999 47.45812832900003, -122.25936560499997 47.458393055000045, -122.25933595199996 47.45839293500006, -122.25933501299994 47.45850154000004, -122.25911755099997 47.458500654000034, -122.25911737399997 47.458521017000066, -122.25919645099998 47.45852133900007, -122.25919621699995 47.458548491000045, -122.25915667899994 47.45854833000004, -122.25915650199994 47.458568693000075, -122.25857330899998 47.45856631400005, -122.25857313199998 47.458586678000074, -122.25900805699996 47.45858845300006, -122.25900741099997 47.45866311900005, -122.25938302799995 47.45866465000006, -122.25946754599994 47.458607295000036, -122.25974431599997 47.458608423000044, -122.25974449299997 47.45858806000007, -122.259922416 47.45858878400003, -122.259923706 47.45843945100006, -122.25988416699994 47.45843929000006, -122.25988756599997 47.458045595000044, -122.25985791199997 47.45804547400007, -122.25986131199994 47.45765177900006, -122.26074103399998 47.45765535900006, -122.26074115099999 47.45764178300004, -122.26091907199998 47.45764250700006, -122.26091825199995 47.45773753600008, -122.26097755899997 47.457737777000034, -122.26097709099997 47.45779208000005, -122.26102651399998 47.45779228100008, -122.26102464299998 47.45800949200003, -122.26098510499997 47.45800933100003, -122.26098241399995 47.458321572000045, -122.26115045199998 47.45832225500004, -122.26114483799995 47.45897388900005, -122.26116460799994 47.458973969000056, -122.26116337999997 47.45911651500006, -122.26120291899997 47.459116675000075, -122.26120180799995 47.45924564500007, -122.26122157699996 47.45924572500007, -122.26121742499998 47.45972766500006, -122.26118777199997 47.45972754400003, -122.26118666099995 47.459856513000034, -122.26038598799994 47.459853259000056, -122.26038832999996 47.45958174400005, -122.26024005899995 47.45958114000007, -122.26024052699995 47.459526837000055, -122.25990444399997 47.459525470000074, -122.25990368199996 47.45961371200008, -122.25987402799996 47.45961359100005, -122.25987262099994 47.45977650000003, -122.2596255 47.45977549400004, -122.25962467899996 47.45987052400005, -122.258725159 47.459866858000055, -122.25872598099994 47.459771826000065, -122.25843932099997 47.459770657000035, -122.25844090699997 47.45958738400003, -122.25653314099998 47.45957958400004, -122.25653137199998 47.45978322000008, -122.25656102699998 47.45978334100005, -122.25655902199998 47.46001412900006, -122.25651948199999 47.460013968000055, -122.25651877499996 47.46009542200005, -122.25640015699997 47.460094937000065, -122.25630071899997 47.460162410000066, -122.25564831499997 47.46015973600004, -122.25564996899999 47.459969674000035, -122.25562031499999 47.459969553000064, -122.25562178999996 47.459799856000075, -122.25566132899996 47.45980001800007, -122.25566634999996 47.45922304800007, -122.25563669499996 47.45922292600005, -122.25564372199995 47.45841517100007, -122.25566349099995 47.458415253000055, -122.25566502599997 47.45823876900005, -122.25569467999998 47.458238890000075, -122.25569538899998 47.45815743600008, -122.25572504299998 47.45815755800004, -122.25572616399995 47.45802858800005, -122.25580523999997 47.45802891300008, -122.25580535799998 47.45801533700006, -122.25650716299998 47.45801821400005, -122.25650792899995 47.457929972000045, -122.25646838999995 47.45792981000005, -122.25646892099996 47.45786871900003, -122.255806657 47.45786600500003, -122.25581297299999 47.45713970700007, -122.25686072299999 47.457144, -122.25686042899997 47.457177939000076, -122.25692961899995 47.45717822200004, -122.25692555499995 47.45764658200005, -122.25681617599997 47.45772080300003, -122.25679161199997 47.45770373300007, -122.25668720699997 47.457774580000034, -122.25666264199998 47.45775750900003, -122.25654332099998 47.45783847700005, -122.25660227499998 47.45787944700004, -122.25657741499998 47.457896316000074, -122.25669040999998 47.457974841000066, -122.25686441999994 47.45785676200006, -122.25703245799997 47.45785745100005, -122.25703210499995 47.45789817700006, -122.25717048799999 47.45789874400003, -122.25717154799997 47.45777656300004, -122.25714207099998 47.457756078000045, -122.25726139299996 47.457675109000036, -122.25737012199994 47.45767555400005, -122.25737035799995 47.457648402000075, -122.25779539199999 47.457650140000055, -122.25779556699996 47.45762977600003, -122.25821071699994 47.45763147300005, -122.25821048099999 47.45765862400003, -122.25855643899996 47.45766003600005, -122.25861048099995 47.457697591000056, -122.25868008399999 47.45765035900007, -122.25890742699994 47.45765128600004, -122.25890731099997 47.45766486200006, -122.259105 47.45766566800006, -122.25910517699998 47.45764530500003, -122.25926332799997 47.45764594900004, -122.25926350499998 47.45762558600006, -122.25942165699996 47.45762623000007, -122.25942194999999 47.457592291000026, -122.25959986999999 47.45759301600003, -122.25959875599995 47.457721985000035, -122.25948014299996 47.45772150200003, -122.25947850099999 47.45791156100006, -122.25937965499998 47.45791115800006, -122.25937989 47.457884007000075, -122.25934035099999 47.457883845000026, -122.25933823999998 47.45812820800006, -122.25936789299999 47.45812832900003), (-122.25764253399996 47.458178985000075, -122.25770184199996 47.45817922800006, -122.25770148799995 47.45821995500006, -122.25779044899997 47.45822031900008, -122.25779133299994 47.45811850100006, -122.25783087099995 47.45811866200006, -122.25783310599996 47.45786072400006, -122.25771449099994 47.45786024000006, -122.25771478499996 47.45782630100007, -122.25755663299998 47.45782565400003, -122.25755592699994 47.45790710800003, -122.25736812099996 47.45790634000008, -122.25736800299995 47.45791991500005, -122.25744707899997 47.45792024000008, -122.25744660799995 47.457974541000056, -122.25747626099997 47.45797466300007, -122.25747573199999 47.458035754000036, -122.25759434699995 47.45803623900008, -122.25759411099995 47.45806339000006, -122.25762376499995 47.458063511000034, -122.257623 47.45815175300004, -122.25767242199998 47.45815195500006, -122.25767236499996 47.45815874300007, -122.25764270999997 47.45815862200004, -122.25764253399996 47.458178985000075)) (255)
        #      POLYGON ((-122.33634393299997 47.613014915000065, -122.33632050499995 47.61298538200003, -122.33629588499997 47.612994558000025, -122.33613523899999 47.61279204300007, -122.33610446399996 47.61280351200003, -122.33608438299996 47.61277819900005, -122.33604129699995 47.612794257000076, -122.33588400099995 47.61259596000008, -122.335853225 47.61260743100007, -122.33580637199998 47.61254836300003, -122.33573866499995 47.61257359800004, -122.33574535899999 47.61258203600005, -122.33577613399996 47.612570566000045, -122.33579621499996 47.61259588000007, -122.33574081799998 47.61261652600007, -122.33571404399999 47.61258277400003, -122.33570173399994 47.612587362000056, -122.33593935099998 47.612886916000036, -122.33597012699994 47.61287544600003, -122.33615085199995 47.61310327600006, -122.33567074799998 47.61328221100007, -122.335664055 47.61327377300006, -122.33553479499994 47.61332194700003, -122.33555152799994 47.61334304300004, -122.33542226899999 47.61339121800006, -122.33540888199997 47.61337434100005, -122.33533501899996 47.61340186900003, -122.33532832599997 47.61339343100008, -122.33526061799995 47.61341866500004, -122.33521711099996 47.61336381600006, -122.33529712899997 47.61333399500006, -122.33525696899994 47.613283366000076, -122.33514617399999 47.61332465800007, -122.33501565299997 47.61316011300005, -122.33497872099997 47.613173876000076, -122.33488166799998 47.61305152300008, -122.33485089099997 47.61306299200004, -122.33471033199999 47.612885790000064, -122.33467955499998 47.61289726100006, -122.33447206299996 47.61263567600008, -122.33469980399997 47.612550799000076, -122.33468641699994 47.61253392200007, -122.33539425899994 47.61227011300008, -122.33558836699996 47.61251481900007, -122.33560683199994 47.61250793700003, -122.33562691299994 47.61253325200005, -122.33576848099995 47.61248048900006, -122.33553086499995 47.612180935000026, -122.33611559799999 47.61196300200004, -122.33610555799999 47.611950345000025, -122.33627174499998 47.61188840600005, -122.33625501099999 47.611867311000026, -122.33645812799995 47.61179160700004, -122.33650832799998 47.61185489300004, -122.33653910299995 47.61184342300004, -122.33665289299995 47.61198687000007, -122.33667751299998 47.611977694000075, -122.33697537499995 47.61235318900003, -122.33673532699999 47.61244265800008, -122.33678218099999 47.612501724000026, -122.33636363399995 47.61265772000007, -122.33639040899999 47.61269147300004, -122.33642733999994 47.61267770900008, -122.33649427499995 47.61276209000005, -122.33663584299995 47.61270932700006, -122.336602374 47.612667137000074, -122.33671316599998 47.612625843000046, -122.33673659399994 47.61265537600008, -122.33693355699995 47.61258196600005, -122.33686662199995 47.61249758500003, -122.33695894799996 47.61246317300004, -122.33696564199994 47.612471611000046, -122.33703950299997 47.61244408300007, -122.33718676199999 47.61262972000003, -122.33714983099998 47.612643485000035, -122.33717325899994 47.612673018000066, -122.337068623 47.612712017000035, -122.33708870299995 47.61273733200005, -122.33634393299997 47.613014915000065)) (239)
        #      POLYGON ((-122.20177824299998 47.61723635000004, -122.20039163099995 47.61726512600006, -122.20039043199995 47.617237972000055, -122.20034090999997 47.61723899900005, -122.20032801899998 47.616947088000074, -122.20058553099994 47.61694174500008, -122.20058373299997 47.616901013000074, -122.20043516799996 47.61690409600004, -122.20043276999996 47.616849787000035, -122.20046248299997 47.61684917000008, -122.20043729899999 47.616278926000064, -122.20047691599996 47.61627810300007, -122.20047361799999 47.61620342900005, -122.20056275599995 47.61620157900006, -122.20055945799999 47.616126904000055, -122.20062878799996 47.61612546600003, -122.20062758799997 47.61609831200008, -122.20054835399998 47.61609995600003, -122.20054595599998 47.61604564700008, -122.20063509399995 47.61604379700003, -122.20063389399996 47.61601664300008, -122.20121824099999 47.61600451700008, -122.20122843699994 47.61623532900006, -122.20147604199997 47.61623019100006, -122.20149583699998 47.616678239000066, -122.201307655 47.61668214500003, -122.20130885499998 47.616709300000025, -122.20173473899996 47.61670046000006, -122.20173353999996 47.61667330600005, -122.20192172099996 47.616669400000035, -122.20190522299998 47.61629602600004, -122.20311353799997 47.61627093800007, -122.20310093499995 47.615985816000034, -122.20301179699999 47.61598766600008, -122.20301719799994 47.61610986200003, -122.20189802399995 47.61613309900008, -122.20188062699998 47.61573935900003, -122.20207870999997 47.61573524700003, -122.20206701099994 47.61547049200004, -122.20187883299997 47.61547439900005, -122.20186713499999 47.61520964300007, -122.20295658199996 47.61518702400008, -122.20296198199998 47.61530921900004, -122.20326900899994 47.61530284200006, -122.20325220299998 47.61492268200004, -122.20320268299997 47.61492371000003, -122.20318827899996 47.61459785900007, -122.20331703099998 47.61459518600003, -122.20331553099999 47.614561243000026, -122.20324620299999 47.61456268200004, -122.20322069599996 47.61398565400003, -122.20400309999997 47.613969403000056, -122.20400370099998 47.61398298000006, -122.20458802999997 47.613970840000036, -122.20459673499994 47.614167708000025, -122.20455711899996 47.614168531000075, -122.20457363199995 47.61454190200004, -122.20434584199995 47.61454663400008, -122.204365355 47.614987892000045, -122.20444458699995 47.61498624500007, -122.204447889 47.615060920000076, -122.20434884899998 47.615062977000036, -122.204355153 47.615205538000055, -122.20516728399997 47.61518866200004, -122.20520001699998 47.615928619000044, -122.20505145499999 47.61593170500004, -122.20505775999999 47.616074266000055, -122.20515680299997 47.61607220800005, -122.20515860299997 47.61611293900006, -122.20532697399995 47.61610944000006, -122.20532907699999 47.61615696100006, -122.20506166399997 47.61616251800007, -122.20506286499995 47.616189672000075, -122.20519161899995 47.61618699700006, -122.20521654399994 47.61675045100003, -122.20527597099999 47.61674921600007, -122.20527867299995 47.61681031400008, -122.20530838599996 47.616809697000065, -122.20532490399995 47.617183071000056, -122.205180843 47.617287988000044, -122.20456677099997 47.61730074800005, -122.20456557099999 47.61727359300005, -122.20349589899996 47.617295814000045, -122.20347128899999 47.61673914700003, -122.20322368199999 47.61674428900005, -122.20323088399999 47.61690721600007, -122.20328040599998 47.61690618700004, -122.20328580799998 47.61702838300005, -122.20321647699996 47.61702982300005, -122.20321917799998 47.61709092000007, -122.20270415299996 47.61710161500008, -122.20270145199999 47.61704051800007, -122.20274106899996 47.617039695000074, -122.20273986899997 47.617012540000076, -122.20278939199994 47.61701151200003, -122.20278849099998 47.61699114600003, -122.20258049999995 47.61699546500006, -122.20257929999997 47.61696831000006, -122.20240102199995 47.616972012000076, -122.20240222199999 47.616999167000074, -122.20232298799999 47.61700081200007, -122.202325987 47.617068698000026, -122.20265283099997 47.617061912000054, -122.20265553099995 47.61712300900007, -122.20327950399997 47.617110052000044, -122.20328730699998 47.61728655600007, -122.20178184299999 47.617317814000046, -122.20177824299998 47.61723635000004)) (233)
        #      POLYGON ((-122.32410413399998 47.60959053600004, -122.32392205999997 47.609379802000035, -122.32384972499995 47.60940916900006, -122.32367479099997 47.609206699000026, -122.32381946099997 47.60914796600008, -122.32373735099998 47.60905292900003, -122.32409902399996 47.608906096000055, -122.32414543499999 47.60895981200008, -122.32445888399997 47.60883255600004, -122.32486944599998 47.60930773800004, -122.32446557699996 47.60947170400004, -122.32444772699995 47.60945104400008, -122.32410413399998 47.60959053600004)) (180)
        #json_data['POLYGON_WKT'] = raw_data['POLYGON_WKT']

        # columnName: POLYGON_CLASS
        # 72.45 populated, 0.0 unique
        #      OWNED_POLYGON (114056)
        #      SHARED_POLYGON (89986)
        #json_data['POLYGON_CLASS'] = raw_data['POLYGON_CLASS']

        # columnName: ENCLOSED
        # 85.32 populated, 0.0 unique
        #      FALSE (226678)
        #      TRUE (13607)
        #json_data['ENCLOSED'] = raw_data['ENCLOSED']

        # columnName: PHONE_NUMBER
        # 72.66 populated, 83.37 unique
        #      +12066844075 (348)
        #      +18583244111 (153)
        #      +15096638711 (85)
        #      +12537527320 (72)
        #      +15419554700 (68)
        json_data['PHONE_NUMBER'] = raw_data['PHONE_NUMBER']

        # columnName: IS_SYNTHETIC
        # 85.32 populated, 0.0 unique
        #      FALSE (234226)
        #      TRUE (6059)
        #json_data['IS_SYNTHETIC'] = raw_data['IS_SYNTHETIC']

        # columnName: INCLUDES_PARKING_LOT
        # 79.89 populated, 0.0 unique
        #      FALSE (220437)
        #      TRUE (4565)
        #json_data['INCLUDES_PARKING_LOT'] = raw_data['INCLUDES_PARKING_LOT']

        # columnName: ISO_COUNTRY_CODE
        # 100.0 populated, 0.0 unique
        #      US (281628)
        json_data['BUSINESS_ADDR_COUNTRY'] = raw_data['ISO_COUNTRY_CODE']

        # columnName: WKT_AREA_SQ_METERS
        # 85.44 populated, 6.27 unique
        #      1236 (5076)
        #      203 (627)
        #      142 (616)
        #      135 (609)
        #      162 (596)
        #json_data['WKT_AREA_SQ_METERS'] = raw_data['WKT_AREA_SQ_METERS']

        # columnName: ALT_ADDRESS
        # 100.0 populated, 0.19 unique
        #      [] (239343)
        #json_data['ALT_ADDRESS'] = raw_data['ALT_ADDRESS']

        # columnName: BUILDING_NAME
        # 0.0 populated, 80.0 unique
        #      Keyport Junction (2)
        #      Feeder: River Valley Dr (1)
        #      1 Stop Plaza (1)
        #      Ykk Building (1)
        #json_data['BUILDING_NAME'] = raw_data['BUILDING_NAME']

        # columnName: CENSUS_CODE
        # 100.0 populated, 1.86 unique
        #      530330262001 (2102)
        #      530330292061 (1224)
        #      530330072021 (1189)
        #      530330081021 (1180)
        #      530330081022 (1036)
        #json_data['CENSUS_CODE'] = raw_data['CENSUS_CODE']
        # columnName: FULL_ADDRESS
        # 92.74 populated, 66.87 unique
        #      1100 9th Ave\nSeattle WA 98101-2756 (452)
        #      17801 International Blvd\nSeatac WA 98158-1202 (157)
        #      3000 184th St SW\nLynnwood WA 98037-4718 (110)
        #      1145 Broadway\nSeattle WA 98122-4201 (110)
        #      4502 S Steele St\nTacoma WA 98409-7242 (102)
        json_data['BUSINESS_ADDR_FULL'] = raw_data['FULL_ADDRESS'].replace('\n', ' ')

        # columnName: IS_INTERSECTION
        # 89.97 populated, 0.0 unique
        #      FALSE (233048)
        #      TRUE (20345)
        json_data['IS_INTERSECTION'] = 'Yes' if raw_data['IS_INTERSECTION'] == 'TRUE' else ''

        # columnName: MAILING_VERIFIED_ID
        # 82.73 populated, 61.09 unique
        #      981012756001 (453)
        #      981581202013 (164)
        #      984097242997 (126)
        #      980374718995 (122)
        #      981224201452 (111)
        #json_data['MAILING_VERIFIED_ID'] = raw_data['MAILING_VERIFIED_ID']

        # columnName: MAILING_VERIFIED_STATUS
        # 99.99 populated, 0.0 unique
        #      VERIFIED_DELIVERY_POINT (176942)
        #      VERIFIED_PREMISE (56046)
        #      NON_VERIFIED (48618)
        json_data['MAILING_VERIFIED_STATUS'] = raw_data['MAILING_VERIFIED_STATUS']

        # columnName: POSTAL_CODE_EXTRA
        # 86.27 populated, 3.87 unique
        #      2756 (465)
        #      9800 (348)
        #      0001 (314)
        #      4201 (211)
        #      1202 (201)
        #json_data['POSTAL_CODE_EXTRA'] = raw_data['POSTAL_CODE_EXTRA']

        # columnName: RELATED_PARKING
        # 100.0 populated, 19.7 unique
        #      [] (86070)
        #json_data['RELATED_PARKING'] = raw_data['RELATED_PARKING']

        # columnName: STREET
        # 99.83 populated, 9.7 unique
        #      Main St (2406)
        #      1st Ave S (1346)
        #      Highway 99 (1308)
        #      Broadway (1252)
        #      E Sprague Ave (1236)
        #json_data['STREET'] = raw_data['STREET']

        # columnName: STREET_EXTRA
        # 7.22 populated, 37.32 unique
        #      Safeway (56)
        #      Broadway (45)
        #      9th Ave (38)
        #      4th Ave (35)
        #      5th Ave (35)
        #json_data['STREET_EXTRA'] = raw_data['STREET_EXTRA']

        # columnName: STREET_PREFIX
        # 0.35 populated, 2.11 unique
        #      Highway (605)
        #      State Highway (228)
        #      Us (73)
        #      Interstate (45)
        #      Avenue (12)
        #json_data['STREET_PREFIX'] = raw_data['STREET_PREFIX']

        # columnName: SUB_BUILDING
        # 21.13 populated, 10.71 unique
        #      Ste A (2976)
        #      Ste B (2569)
        #      Ste 100 (2329)
        #      Ste 101 (2080)
        #      Ste 200 (1809)
        #json_data['SUB_BUILDING'] = raw_data['SUB_BUILDING']

        # columnName: SUB_REGION
        # 92.73 populated, 0.02 unique
        #      King (90603)
        #      Pierce (27352)
        #      Snohomish (25207)
        #      Spokane (18771)
        #      Clark (13856)
        #json_data['SUB_REGION'] = raw_data['SUB_REGION']

        #--remove empty attributes and capture the stats
        json_data = self.remove_empty_tags(json_data)
        self.capture_mapped_stats(json_data)

        return json_data

    #----------------------------------------
    def load_reference_data(self):

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value

    #-----------------------------------
    def compute_record_hash(self, target_dict, attr_list = None):
        if attr_list:
            string_to_hash = ''
            for attr_name in sorted(attr_list):
                string_to_hash += (' '.join(str(target_dict[attr_name]).split()).upper() if attr_name in target_dict and target_dict[attr_name] else '') + '|'
        else:           
            string_to_hash = json.dumps(target_dict, sort_keys=True)
        return hashlib.md5(bytes(string_to_hash, 'utf-8')).hexdigest()

    #----------------------------------------
    def format_date(self, raw_date):
        try: 
            return datetime.strftime(dateparse(raw_date), '%Y-%m-%d')
        except: 
            self.update_stat('!INFO', 'BAD_DATE', raw_date)
            return ''

    #----------------------------------------
    def remove_empty_tags(self, d):
        if isinstance(d, dict):
            for  k, v in list(d.items()):
                if v is None or len(str(v).strip()) == 0:
                    del d[k]
                else:
                    self.remove_empty_tags(v)
        if isinstance(d, list):
            for v in d:
                self.remove_empty_tags(v)
        return d

    #----------------------------------------
    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]['count'] = 0

        self.stat_pack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]['examples'] = []
            if example not in self.stat_pack[cat1][cat2]['examples']:
                if len(self.stat_pack[cat1][cat2]['examples']) < 5:
                    self.stat_pack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]['examples'][randomSampleI] = example
        return

    #----------------------------------------
    def capture_mapped_stats(self, json_data):

        if 'DATA_SOURCE' in json_data:
            data_source = json_data['DATA_SOURCE']
        else:
            data_source = 'UNKNOWN_DSRC'

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(data_source, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(data_source, key2, subrecord[key2])

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return

#----------------------------------------
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    input_file = '2023-05-03 WA sample.csv'
    csv_dialect = 'excel'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', dest='input_file', default = input_file, help='the name of the input file')
    parser.add_argument('-o', '--output_file', dest='output_file', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    args = parser.parse_args()

    if not args.input_file or not os.path.exists(args.input_file):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)
    if not args.output_file:
        print('\nPlease supply a valid output file name on the command line\n') 
        sys.exit(1)

    input_file_handle = open(args.input_file, 'r')
    output_file_handle = open(args.output_file, 'w', encoding='utf-8')
    mapper = mapper()

    input_row_count = 0
    output_row_count = 0
    for input_row in csv.DictReader(input_file_handle, dialect=csv_dialect):
        input_row_count += 1

        json_data = mapper.map(input_row, input_row_count)
        if json_data:
            output_file_handle.write(json.dumps(json_data) + '\n')
            output_row_count += 1

        if input_row_count % 1000 == 0:
            print('%s rows processed, %s rows written' % (input_row_count, output_row_count))
        if shut_down:
            break

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
    print('%s rows processed, %s rows written, %s\n' % (input_row_count, output_row_count, run_status))

    output_file_handle.close()
    input_file_handle.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)


    sys.exit(0)

