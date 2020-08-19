from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

def get_info_from_elas(N, topic):
    # Tao doi tuong elastic_client
    elastic_client = Elasticsearch(hosts=["localhost"])
    # list luu du lieu so document va status_id thoa man truy van 
    final_list = []
    # Luu tru thong tin cac ngay: (day[0]: hom nay, day[1]: 1 ngay truoc, ... day[N]: N ngay truoc)
    day = []

    for i in range(0,N):
        
        date_N_days_ago = datetime.now() - timedelta(days=i)
        day.append(date_N_days_ago.strftime("%Y-%m-%d"))
        query_body = {
        "query" : {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "content": "{}".format(topic)
                            }
                        },
                        {
                            "match_phrase": {
                                "date": "{}".format(day[i])
                            }
                        }
                    ]
                }
            },
            "size": 10000
        }
    
        result = elastic_client.search(index="status_fb", body=query_body, scroll='50m' )
        # num_doc: So luong bai viet thoa man
        num_doc = result['hits']['total']['value']

        final_list.append([num_doc])

        # So luong fb_id max tra ve la 10000
        if num_doc < 10000:
            for j in range(0, num_doc):
                fb_id = result['hits']['hits'][j]['_id']
                final_list[i].append(fb_id)
        else:
            for j in range(0, 10000):
                fb_id = result['hits']['hits'][j]['_id']
                final_list[i].append(fb_id)
    #print test_list
    for i in range(0, len(final_list)):
        print(day[i],final_list[i])
        print("\n")
    return day, final_list
if __name__ == "__main__":
    N = 45
    topic = "corona"
    get_info_from_elas(N, topic)
