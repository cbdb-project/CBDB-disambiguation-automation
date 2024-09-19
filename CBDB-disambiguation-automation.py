# pip install char_converter

import sqlite3
import sys
import csv
import re
import pandas as pd
from char_converter import CharConverter

# setup SQLite database name
dbName = "latest.db"

# setup content file name
contentsName = "input.txt"

# output compare result
compareResultListFile = "compare-result-list"

conn = sqlite3.connect(dbName)
c = conn.cursor()
# for row in c.execute("SELECT * FROM BIOG_MAIN LIMIT 5"):
#         print(row)

# setup char converter
converter = CharConverter("v2s")

# read nianhao list
nianhaoList = []
with open("nianhao.txt", "r", encoding="utf-8") as f:
    for row in f:
        nianhaoList.append(row.strip())

class setupConditions:

    def runQuery(self, SQL):
        output = []
        for row in c.execute(SQL):
            output.append(row[0])
        return output

    def setupDy(self, dyList):
        IDJoin = '" or c_dy="'.join(dyList)
        IDJoin = '"' + IDJoin + '"'
        SQL = "SELECT c_personid FROM BIOG_MAIN WHERE c_dy = %s" % IDJoin
        print(SQL)
        return self.runQuery(SQL)

    def setupIndexYear(self, infYear, supYear):
        SQL = (
            "SELECT c_personid FROM BIOG_MAIN WHERE c_index_year >= %s and c_index_year <= %s"
            % (infYear, supYear)
        )
        return self.runQuery(SQL)

    def mergeLists(self, dataList):
        output = []
        for i in dataList:
            output.extend(i)
        output = list(set(output))
        return output


# converter.convert(text)
class getVariousDataTypes:

    def runQuery(self, SQL, idNameMapping=None):
        dataList = []
        for row in c.execute(SQL):
            if idNameMapping == 1:
                pass
            if idNameMapping == 1 and normalizeNameSetting == 1:
                row = [row[0], converter.convert(row[1])]
            dataList.append(row)
        dataDict = {}
        count = 0
        for i in dataList:
            count += 1
            if i[0] not in dataDict:
                if len(i) < 2:
                    print("输出的资料少于两列")
                    sys.exit()
                if i[1] == None:
                    continue
                if len(i) == 2:
                    if normalizeBiogSetting == 1 and idNameMapping == None:
                        dataDict[i[0]] = converter.convert(i[1])
                    else:
                        dataDict[i[0]] = i[1]

                else:
                    i = [j for j in i if j is not None]
                    if normalizeBiogSetting == 1 and idNameMapping == None:
                        dataDict[i[0]] = converter.convert(";".join(i[1:]))
                    else:
                        dataDict[i[0]] = ";".join(i[1:])

            else:
                if i[1] == None:
                    continue
                if len(i) == 2:
                    if normalizeBiogSetting == 1 and idNameMapping == None:
                        try:
                            dataDict[i[0]] = (
                                dataDict[i[0]] + ";" + converter.convert(i[1])
                            )
                        except:
                            print(i[0])
                            print(i[1])
                            sys.exit()
                    else:
                        dataDict[i[0]] = dataDict[i[0]] + ";" + i[1]
                else:
                    new_i = []
                    for j in i:
                        if j is not None:
                            if normalizeBiogSetting == 1 and idNameMapping == None:
                                if isinstance(j, int):
                                    new_i.append(j)
                                else:
                                    new_i.append(converter.convert(j))
                            else:
                                new_i.append(j)
                    i = new_i
                    # i = [j for j in i if j is not None]
                    # 譬如官职
                    # 如果希望 地名1;官名1;;地名2;官名2 则形如
                    # dataDict[i[0]] = dataDict[i[0]] + ";;"+ ";".join(i[1:])
                    # 如果希望 地名1;官名1;地名2;官名2 则形如
                    # dataDict[i[0]] = dataDict[i[0]] + ";"+ ";".join(i[1:])
                    dataDict[i[0]] = dataDict[i[0]] + ";" + ";".join(i[1:])

        return dataDict

    def convertListToString(self, dataList):
        dataList = [str(i) for i in dataList]
        dataList = ",".join(dataList)
        # dataList = "\""+dataList+"\""
        return dataList

    def altname(self, personIDList):
        personIDListString = self.convertListToString(personIDList)
        SQL = """SELECT DISTINCT ALTNAME_DATA.c_personid, ALTNAME_DATA.c_alt_name_chn
        FROM ALTNAME_DATA
        WHERE c_personid in (%s);
        """ % (
            personIDListString
        )
        return self.runQuery(SQL)

    def biogAddr(self, personIDList):
        personIDListString = self.convertListToString(personIDList)
        if addrBelongsMatchForBiogAddr == 1:
            SQL = """SELECT DISTINCT BIOG_ADDR_DATA.c_personid, ADDR_CODES.c_name_chn
                                    FROM BIOG_ADDR_DATA INNER JOIN ADDR_CODES ON BIOG_ADDR_DATA.c_addr_id = ADDR_CODES.c_addr_id
                                    WHERE
                                    (BIOG_ADDR_DATA.c_addr_type = -1 or BIOG_ADDR_DATA.c_addr_type = 0 or BIOG_ADDR_DATA.c_addr_type = 1 or BIOG_ADDR_DATA.c_addr_type = 14)
                                    AND
                                    (c_personid in (%s))
                    UNION
                    SELECT DISTINCT BIOG_MAIN.c_personid, ADDR_CODES.c_name_chn
                                    FROM BIOG_MAIN INNER JOIN ADDR_CODES ON BIOG_MAIN.c_index_addr_id = ADDR_CODES.c_addr_id
                                    WHERE (BIOG_MAIN.c_index_addr_id IS NOT NULL
                                            AND BIOG_MAIN.c_index_addr_id != 0)
                                    AND
                                    (c_personid in (%s))
                    UNION
                    SELECT DISTINCT BIOG_ADDR_DATA.c_personid, ADDR_CODES_FOR_BELONGS.c_name_chn
                                    FROM BIOG_ADDR_DATA INNER JOIN ADDR_CODES ON BIOG_ADDR_DATA.c_addr_id = ADDR_CODES.c_addr_id
                                                    INNER JOIN ADDR_BELONGS_DATA ON ADDR_CODES.c_addr_id = ADDR_BELONGS_DATA.c_addr_id
                                                    INNER JOIN ADDR_CODES AS ADDR_CODES_FOR_BELONGS ON ADDR_CODES_FOR_BELONGS.c_addr_id = ADDR_BELONGS_DATA.c_belongs_to
                                    WHERE
                                    (BIOG_ADDR_DATA.c_addr_type = -1 or BIOG_ADDR_DATA.c_addr_type = 0 or BIOG_ADDR_DATA.c_addr_type = 1 or BIOG_ADDR_DATA.c_addr_type = 14)
                                    AND
                                    (c_personid in (%s))
                    UNION
                    SELECT DISTINCT BIOG_MAIN.c_personid, ADDR_CODES_FOR_BELONGS.c_name_chn
                                    FROM BIOG_MAIN INNER JOIN ADDR_CODES ON BIOG_MAIN.c_index_addr_id = ADDR_CODES.c_addr_id
                                                    INNER JOIN ADDR_BELONGS_DATA ON BIOG_MAIN.c_index_addr_id = ADDR_BELONGS_DATA.c_addr_id
                                                    INNER JOIN ADDR_CODES AS ADDR_CODES_FOR_BELONGS ON ADDR_CODES_FOR_BELONGS.c_addr_id = ADDR_BELONGS_DATA.c_belongs_to
                                    WHERE (BIOG_MAIN.c_index_addr_id IS NOT NULL
                                            AND BIOG_MAIN.c_index_addr_id != 0)
                                    AND
                                    (c_personid in (%s))""" % (
                personIDListString,
                personIDListString,
                personIDListString,
                personIDListString,
            )

        else:
            SQL = """SELECT DISTINCT BIOG_ADDR_DATA.c_personid, ADDR_CODES.c_name_chn
                FROM BIOG_ADDR_DATA INNER JOIN ADDR_CODES ON BIOG_ADDR_DATA.c_addr_id = ADDR_CODES.c_addr_id
                WHERE
                (c_addr_type = -1 or c_addr_type = 0 or c_addr_type = 1 or c_addr_type = 14)
                AND
                (c_personid in (%s))
                UNION
                SELECT DISTINCT BIOG_MAIN.c_personid, ADDR_CODES.c_name_chn
                FROM BIOG_MAIN INNER JOIN ADDR_CODES ON BIOG_MAIN.c_index_addr_id = ADDR_CODES.c_addr_id
                WHERE BIOG_MAIN.c_index_addr_id is NOT NULL
                AND
                (c_personid in (%s))
                """ % (
                personIDListString,
                personIDListString,
            )
        return self.runQuery(SQL)

    def posting(self, personIDList):
        personIDListString = self.convertListToString(personIDList)
        SQL = """SELECT DISTINCT [POSTED_TO_OFFICE_DATA].[c_personid],
        [OFFICE_CODES].[c_office_chn],
        [ADDR_CODES].[c_name_chn]
        FROM [POSTED_TO_OFFICE_DATA]
        LEFT JOIN [POSTED_TO_ADDR_DATA] ON [POSTED_TO_ADDR_DATA].[c_posting_id] =
        [POSTED_TO_OFFICE_DATA].[c_posting_id]
        LEFT JOIN [OFFICE_CODES] ON [OFFICE_CODES].[c_office_id] =
        [POSTED_TO_OFFICE_DATA].[c_office_id]
        LEFT JOIN [ADDR_CODES] ON [ADDR_CODES].[c_addr_id] =
        [POSTED_TO_ADDR_DATA].[c_addr_id]
        WHERE [POSTED_TO_OFFICE_DATA].[c_personid] in (%s);
        """ % (
            personIDListString
        )
        return self.runQuery(SQL)

    def kinName(self, personIDList):
        personIDListString = self.convertListToString(personIDList)
        SQL = """SELECT DISTINCT KIN_DATA.c_personid, BIOG_MAIN.c_mingzi_chn
        FROM KIN_DATA INNER JOIN BIOG_MAIN ON KIN_DATA.c_kin_id = BIOG_MAIN.c_personid
        WHERE KIN_DATA.c_personid in (%s)
        """ % (
            personIDListString
        )
        return self.runQuery(SQL)

    def entry(self, personIDList):
        output = {}
        entry_keyword_list = [
            "進士",
            "舉人",
            "狀元",
            "榜眼",
            "探花",
            "貢生",
            "貢士",
            "魁首",
            "博學鴻",
            "監生",
            "廩生",
            "附生",
            "宗室",
            "佾生",
            "制舉",
        ]
        if normalizeBiogSetting == 1:
            entry_keyword_list = [converter.convert(i) for i in entry_keyword_list]
        personIDListString = self.convertListToString(personIDList)
        SQL = """SELECT DISTINCT ENTRY_DATA.c_personid, ENTRY_CODES.c_entry_desc_chn
        FROM ENTRY_CODES INNER JOIN ENTRY_DATA ON ENTRY_CODES.c_entry_code = ENTRY_DATA.c_entry_code
        WHERE ENTRY_DATA.c_personid in (%s)
        """ % (
            personIDListString
        )
        temp_output = self.runQuery(SQL)
        for person_id, entry_content in temp_output.items():
            output[person_id] = []
            entry_content = entry_content.split(";")
            for entry_item in entry_content:
                for entry_keyword in entry_keyword_list:
                    if entry_keyword in entry_item:
                        output[person_id].append(entry_keyword)
            output[person_id] = ";".join(list(set(output[person_id])))
        return output

    def deathNianHao(self, personIDList):
        personIDListString = self.convertListToString(personIDList)
        SQL = """SELECT DISTINCT BIOG_MAIN.c_personid, NIAN_HAO.c_nianhao_chn
        FROM NIAN_HAO INNER JOIN BIOG_MAIN ON NIAN_HAO.c_nianhao_id = BIOG_MAIN.c_dy_nh_code
        WHERE BIOG_MAIN.c_personid in (%s)
        """ % (
            personIDListString
        )
        return self.runQuery(SQL)

    def source(self, personIDList):
        personIDListString = self.convertListToString(personIDList)
        SQL = """SELECT DISTINCT BIOG_MAIN.c_personid, TEXT_CODES.c_title_chn
        FROM BIOG_SOURCE_DATA INNER JOIN BIOG_MAIN ON BIOG_SOURCE_DATA.c_personid = BIOG_MAIN.c_personid
        INNER JOIN TEXT_CODES ON BIOG_SOURCE_DATA.c_textid = TEXT_CODES.c_textid
        WHERE BIOG_MAIN.c_personid in (%s)
        """ % (
            personIDListString
        )
        return self.runQuery(SQL)

    def writing(self, personIDList):
        personIDListString = self.convertListToString(personIDList)
        SQL = """SELECT DISTINCT BIOG_MAIN.c_personid, TEXT_CODES.c_title_chn
        FROM BIOG_TEXT_DATA INNER JOIN BIOG_MAIN ON BIOG_TEXT_DATA.c_personid = BIOG_MAIN.c_personid
        INNER JOIN TEXT_CODES ON BIOG_TEXT_DATA.c_textid = TEXT_CODES.c_textid
        WHERE BIOG_MAIN.c_personid in (%s)
        """ % (
            personIDListString
        )
        return self.runQuery(SQL)

    def idNameMapping(self, personIDList):
        personIDListString = self.convertListToString(personIDList)
        SQL = """SELECT DISTINCT BIOG_MAIN.c_personid, BIOG_MAIN.c_name_chn
        FROM BIOG_MAIN
        WHERE BIOG_MAIN.c_personid in (%s)
        """ % (
            personIDListString
        )
        return self.runQuery(SQL, idNameMapping=1)

    def nameIDMapping(self, idNameMapping):
        output = {}
        for k, v in idNameMapping.items():
            if v not in output:
                output[v] = [k]
            else:
                output[v].append(k)
        return output


def combineAllData(personIDList, variousDataDict):
    output = {}
    for cbdb_id in personIDList:
        if cbdb_id == 0:
            continue
        output[cbdb_id] = {}
        for data_type, data_content in variousDataDict.items():
            if cbdb_id in data_content:
                output[cbdb_id][data_type] = data_content[cbdb_id]
            else:
                output[cbdb_id][data_type] = ""
    return output


class compareCBDBAndContents:

    def readContents(self, contentsName):
        output = []
        with open(contentsName, "r", encoding="utf-8") as f:
            csvReader = csv.reader(f, delimiter="\t")
            for row in csvReader:
                if normalizeBiogSetting == 1:
                    row = [row[0], row[1], converter.convert(row[2])]
                if normalizeNameSetting == 1:
                    row[1] = converter.convert(row[1])
                output.append(row)
        return output

    def compareByDatapoints(self, allCBDBDataDict, hitID, contents):
        output = {}
        if hitID in allCBDBDataDict:
            score_sum = 0
            for data_type, data_content in allCBDBDataDict[hitID].items():
                score = 0
                catchTermsList = []
                datapointList = data_content.split(";")
                for datapointContentInCBDB in datapointList:
                    if datapointContentInCBDB == "":
                        continue
                    if datapointContentInCBDB in contents:
                        score += 1
                        score_sum += 1
                        catchTermsList.append(datapointContentInCBDB)
                output[data_type] = [score, ";".join(catchTermsList)]
            output["score_sum"] = score_sum
        return output

    def compareCBDBAndContentsComparing(
        self, allCBDBDataDict, nameIDMapping, contentsList
    ):
        output = []
        for i in contentsList:
            contentID = i[0]
            contentName = i[1]
            contents = i[2]
            if contentName in nameIDMapping:
                if nameIDMapping[contentName] == 0:
                    continue
                for j in nameIDMapping[contentName]:
                    match_result = self.compareByDatapoints(
                        allCBDBDataDict, j, contents
                    )
                    output.append([contentID, j, contentName, match_result, contents])
        return output

    def writeCompareResult(self, compareResultList, compareResultListFile):
        output = ""
        header = [
            "input_id",
            "cbdb_id",
            "person_name",
            "match_score",
            "altname_score",
            "altname_match",
            "biogaddr_score",
            "biogaddr_match",
            "entry_score",
            "entry_match",
            "contents",
            "posting_score",
            "posting_match",
            "kin_score",
            "kin_match",
            "death_nh_score",
            "death_nh_match",
            "source_score",
            "source_match",
            "writing_score",
            "writing_match",
            "cbdb_dynasty",
            "only_zero_score",
        ]
        data_type_attention = ["altnameList", "biogAddrList", "entryList"]

        # Lookup personid's dynasty id and dynasty name
        person_dy = getVariousDataTypes().runQuery(
            "SELECT DISTINCT BIOG_MAIN.c_personid, CAST(DYNASTIES.c_dy AS TEXT), DYNASTIES.c_dynasty_chn FROM DYNASTIES INNER JOIN BIOG_MAIN ON DYNASTIES.c_dy = BIOG_MAIN.c_dy"
        )
        new_compare_result_list = [header]
        for row in compareResultList:
            new_row = []
            # Create input_id
            new_row.append(row[0])
            # Create cbdb_id
            new_row.append(row[1])
            # Create person_name
            new_row.append(row[2])
            # Create match_score
            new_row.append(row[3]["score_sum"])
            biog_addr_orignal_score = 0
            # Create altname_score, biogaddr_score, entry_score
            for data_type_attention_item in data_type_attention:
                for match_result_keyword, match_result_data in row[3].items():
                    if match_result_keyword == data_type_attention_item:
                        # biogAddrList match the Jiguan and the belongs. If there are multiple matches, we only assign 1 score.
                        if match_result_keyword == "biogAddrList":
                            biog_addr_orignal_score = match_result_data[0]
                            if biog_addr_orignal_score > 0:
                                match_result_data[0] = 1
                            new_row += match_result_data
                        else:
                            new_row += match_result_data
            # Update match_score base on biogAddrList score = 1 or 0
            if biog_addr_orignal_score > 0:
                new_row[3] = row[3]["score_sum"] - biog_addr_orignal_score + 1
            new_row.append(row[-1])
            # Create posting_score, kin_score, death_nh_score
            for match_result_keyword, match_result_data in row[3].items():
                if (
                    match_result_keyword not in data_type_attention
                    and match_result_keyword != "score_sum"
                ):
                    new_row += match_result_data
            # Create Dynasty column
            person_dy_item = ""
            if row[1] in person_dy:
                person_dy_item = person_dy[row[1]]
            new_row.append(person_dy_item)
            # create only_zero_score column for the next step
            new_row.append("")
            new_compare_result_list.append(new_row)
        print("Creating only_zero_score column...")
        # Create only_zero_score column
        compareResultDf = pd.DataFrame(
            new_compare_result_list[1:], columns=new_compare_result_list[0]
        )
        for index, new_row in enumerate(new_compare_result_list[1:]):
            # # If new_row[3] is 0 and if row[0] records in compareResultList(compareResultList each records' first element is input_id), their scores(compareResultList each records' [3]) are all 0.
            # # append yes, otherwise no.
            only_zero_score = "no"
            if new_row[3] == 0:
                # check compareResultDf's input_id column, find new_row[0] in all compareResultDf's input_id column, if all of their scores are 0, then only_zero_score is yes.
                if new_row[0] in compareResultDf["input_id"].values:
                    if (
                        compareResultDf[compareResultDf["input_id"] == new_row[0]][
                            "match_score"
                        ].sum()
                        == 0
                    ):
                        only_zero_score = "yes"
            # new_compare_result_list[index + 1]'s last column is only_zero_score
            new_compare_result_list[index + 1][-1] = only_zero_score

        pd.DataFrame(new_compare_result_list).to_csv(
            compareResultListFile + ".csv", sep=",", index=False, header=False
        )
        pd.DataFrame(new_compare_result_list).to_excel(
            compareResultListFile + ".xlsx", index=False, header=False
        )

def cleanWritingData(writing_dic):
    ouput = {}
    prefix_list = ["重修"]
    for k, v in writing_dic.items():
        if len(v) <= 1: continue
        v_original = v
        # Remove anything within a bracket
        v = re.sub(r"[\(（][^\(（\)）]*?[\)）]", "", v)
        # Match prefix from left and remove it
        for prefix in prefix_list:
            if v.startswith(prefix):
                v = v[len(prefix):]
                break
        # Match nianhao and remove it
        for nianhao in nianhaoList:
            if nianhao in v:
                v = v.replace(nianhao, "")
        # Remove the content after :
        if ":" in v:
            v = v.split(":")[0]
        if "：" in v:
            v = v.split("：")[0]
        if len(v) <=1:
            v = v_original
        ouput[k] = v.strip()
    return ouput


# important variables declare
variousDataDict = {}
idNameMapping = {}
nameIDMapping = {}
contentsList = []
compareResultList = []

setupConditionsClass = setupConditions()

# setup Dynasties
batchIDByDY = setupConditionsClass.setupDy(
    [
        "6",
        "7",
        "8",
        "10",
        "11",
        "12",
        "13",
        "15",
        "16",
        "17",
        "34",
        "38",
        "47",
        "48",
        "49",
        "52",
        "55",
        "57",
        "59",
        "66",
        "78",
        "18",
    ]
)

# setup indexyear
batchIDByIndexYear = setupConditionsClass.setupIndexYear(907, 1300)
personIDList = setupConditionsClass.mergeLists([batchIDByDY, batchIDByIndexYear])

# setup variants setting
normalizeNameSetting = 0
normalizeBiogSetting = 0

# setup address belongs match
addrBelongsMatchForBiogAddr = 0

getVariousDataTypesClass = getVariousDataTypes()

# creating ID Name mapping. Ex id:name
# and Name ID mapping. Ex name:[id1,id2,id3...]
print("Creating ID Name mapping")
idNameMapping = getVariousDataTypesClass.idNameMapping(personIDList)
nameIDMapping = getVariousDataTypesClass.nameIDMapping(idNameMapping)

# get alternative names
print("Collecting alternative name data...")
variousDataDict["altnameList"] = getVariousDataTypesClass.altname(personIDList)

# get jiguan
print("Collecting jiguan data...")
variousDataDict["biogAddrList"] = getVariousDataTypesClass.biogAddr(personIDList)

# get postings
print("Collecting posting data...")
variousDataDict["postingList"] = getVariousDataTypesClass.posting(personIDList)

# get relatives' names
print("Collecting kinship data...")
variousDataDict["kinList"] = getVariousDataTypesClass.kinName(personIDList)

# get entry data
print("Collecting entry data...")
variousDataDict["entryList"] = getVariousDataTypesClass.entry(personIDList)

# get death year Nianhao
print("Collecting death year Nianhao data...")
variousDataDict["deathNianhaoList"] = getVariousDataTypesClass.deathNianHao(
    personIDList
)

# get source data
print("Collecting source data...")
variousDataDict["sourceList"] = getVariousDataTypesClass.source(personIDList)
variousDataDict["sourceList"] = cleanWritingData(variousDataDict["sourceList"])

# get writing data
print("Collecting writing data...")
variousDataDict["writingList"] = getVariousDataTypesClass.writing(personIDList)
variousDataDict["writingList"] = cleanWritingData(variousDataDict["writingList"])

# combine all data
print("Combining data...")
allCBDBDataDict = combineAllData(personIDList, variousDataDict)

# print("Testing id...")
# print(447386 in variousDataDict)
# print(idNameMapping[1])
# print(nameIDMapping["王安石"])
# print(nameIDMapping["王臣"])
# print(variousDataDict["altnameList"][11])
# print(variousDataDict["biogAddrList"][5])
# print(variousDataDict["postingList"][1])
# print(variousDataDict["kinList"][325])
# print(variousDataDict["kinList"][1])
# print(variousDataDict["deathNianhaoList"][1])
# print(allCBDBDataDict[1])

compareCBDBAndContentsClass = compareCBDBAndContents()

# construct contents
print("Constructing contents...")
contentsList = compareCBDBAndContentsClass.readContents(contentsName)

# compare CBDB data in the contents
compareResultList = compareCBDBAndContentsClass.compareCBDBAndContentsComparing(
    allCBDBDataDict, nameIDMapping, contentsList
)
print("%s records were mapped" % len(compareResultList))
# print(compareResultList[:2])


# write file
compareCBDBAndContentsClass.writeCompareResult(compareResultList, compareResultListFile)

print("Finished")
