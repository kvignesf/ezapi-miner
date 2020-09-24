import operator
import db_manager
from pprint import pprint

client, db = db_manager.get_db_connection()

api_ops_ids = ["30a4c21325f948d7959c3609c4f2276b",
               "c7fe5d30449041af8dab99ca0568ff31",
               "6e1e389d49ea4f18a118abaf87dc2a98",
               "b082cf1986484e18b138149e03939326",
               "dbc6181a57534585b0533a5c6ace0cb6",
               "8cd803dc61de40678d3452f0a8d6f2b6",
               "f190ec5343a6406ba1842ef5cf0945a6",
               "4f2bfe2626cb4f619c2a27b7ea221e97",
               "a859caf9b550460d9f0ae607bae47125",
               "bd3fcaa0f39e44a5bc087759b2520881",
               "868b57025297453593bfd2e8f4eb2986",
               "bbb130ecea3843ab84b1c19c898c6d5b",
               "eeccafd8d9e44b109178dcc2017932db",
               "2ecb35ae83c5439388f3c311a196beeb",
               "81c0da07d0e94b6fbab3619b45cbcddf",
               "e0f5b8344d0a4792aa70c717bf25f953",
               "f666cef1feb4406fb685573370c8b85f",
               "31af43c5ed264830a43afa9740eb649e",
               "13345e0b001d43db84c5d4d80e503741",
               "7d7dd6935a4c4ca5bcef7e9a0877aae9",
               "903459ff3e1644e4bcfade362d3b75ad",
               "374f9e8a908f43da83543da0844dd688",
               "b4b1032056f64c1abc72b7742965c993",
               "4d22821e0aab435c9debc44fc6314687",
               "134ceee835bc46bbaf847a7c4aeda022",
               "5048e79d2a644573a46072017815c3fd",
               "18b2a7b3bad9418c85b92a55ae99935f",
               "dcdf822f215646d49e3e3d04f0b1e379",
               "e4b67e58fd7f4f61b430c1cf49c959e4",
               "2a4533e0371a402599744f1b4035b5e5",
               "a40bd74105a5451896071d72a13f56e8",
               "65da507af4ac4501934effc28bda2010",
               "9e8c60997ade4982995aafbbaad7ca5d",
               "8f46ce9d792e4bb2ac381635441250a6",
               "84a214949b594b938ff8ce723c6c4bdd",
               "bc2cb25f44ce4a2ca76106da5e3e8e7b",
               "0ecf8f7904bd4f0e89edbdfd7a70b8a7",
               "1c423e125aee4511b16f1ff43b248261",
               "5452be9de5b04871b935f2f032eab7bc",
               "8372ecca6ea74391a6ad122c1427f5fe",
               "b3e84a619815483e922d52fe69022eed",
               "e4afc795e8de447fb0cb3e114661d7f6",
               "a66e0ea29eeb47859b7fd878a24cc272",
               "57a4c137fbd04cbbbf06b7d4a270690f",
               "b7ae6c3999a5450c935b7d37d68e3043",
               "a0b6dc2553f247ae88188a631f9e2f2b",
               "08d45b2d4e304727be193fa25c23a869",
               "cb020fd45fdf4a07bd03b30e3f625b74",
               "513253bd3cc84decaf5648778798af06",
               "8944c05eddca4d92aed0ac5c4c05ac6d",
               "ceec316a17ca4b919dc609be04038ca1",
               "f2fb3b4c1f7f443db79f406c47654ba4",
               "e8d2bacb6a6a46b189c6333a63031f01",
               "75187702e3a040369ad781fd29107af9",
               "c6fd6acc942849f08725a676c34c4ead",
               "8532f6f5c3d945a4a119568c35b17d46",
               "ea789407cead4762ba05da8c12bdb987",
               "1750e02fe2b64b14b2f7af31fab843d7",
               "552118b8313d433697ec47a8485ca5db",
               "c1694c73207b411ebba73921a465de40",
               "891f1f13bfbd45539a469df5690d1bd5",
               "1de9a11c18e64b88a0e3b20983321a27",
               "a8f2d39226984d34b491850156773a2a"]


def get_keys_type(d, res={}):
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, dict):
                get_keys_type(v, res)
            elif isinstance(v, list):
                item = v[0]
                if isinstance(item, dict):
                    get_keys_type(item, res)
            else:
                res[k] = v

    elif isinstance(d, list):
        item = d[0]
        if isinstance(item, dict):
            get_keys_type(item, res)

    return res


headerVocab = {}
paramVocab = {}


for id in api_ops_ids:
    tescasedata = list(db.testcases.find({'api_ops_id': id}))
    considered = set()

    for td in tescasedata:

        method = td['method']
        endpoint = td['endpoint']
        status = td['status']

        set_entity = endpoint + '__' + method

        if (set_entity not in considered) and ((status == 'default') or (status.isdigit() and int(int(status) / 200) == 1)):
            considered.add(set_entity)

            reqSchema = td['requestSchema']
            resSchema = td['responseSchema']

            reqHeader = reqSchema['header']
            reqSchema.pop('header', None)

            responseSchema = {}

            for r in resSchema:
                if (r['status'] == 'default') or (r['status'].isdigit() and int(int(r['status']) / 200) == 1):
                    responseSchema = r['body']

            requestKeys = get_keys_type(reqSchema, {})
            responseKeys = get_keys_type(responseSchema, {})
            headerKeys = get_keys_type(reqHeader, {})

            for k, v in headerKeys.items():
                tmp = v + "--" + k
                if tmp not in headerVocab:
                    headerVocab[tmp] = 0
                headerVocab[tmp] += 1

            for k, v in requestKeys.items():
                tmp = v + "--" + k
                if tmp not in paramVocab:
                    paramVocab[tmp] = 0
                paramVocab[tmp] += 1

            for k, v in responseKeys.items():
                tmp = v + "--" + k
                if tmp not in paramVocab:
                    paramVocab[tmp] = 0
                paramVocab[tmp] += 1


sortedHeaderVocab = sorted(
    headerVocab.items(), key=operator.itemgetter(1), reverse=True)
sortedParamVocab = sorted(
    paramVocab.items(), key=operator.itemgetter(1), reverse=True)

for t in sortedHeaderVocab:
    print(t[0], t[1])

print("\n\n------------------------\n\n")

for t in sortedParamVocab:
    print(t[0], t[1])
