import sys
import math
from pyspark import SparkConf, SparkContext

#split lines, extract fields and parse 
def parse_line(line):
    fields = line.split("\t")
    
    parent = fields[2]
    title = fields[3]
    category = fields[4]
    rating = float(fields[5])
    helpful = int(fields[6])
    total = int(fields[7])
    verified = 1 if fields[8].upper() == 'Y' else 0  #transform in boolean type
    
    #return key-value
    return (parent, (title, category, 1, rating, verified, helpful, total))


#reduce - calcolo metriche job1
def aggregate_metrics(v1, v2):
    return (
        v1[0],             #title
        v1[1],             #category
        v1[2] + v2[2],     #review_count
        v1[3] + v2[3],     #ratingSum
        v1[4] + v2[4],     #verifiedCount
        v1[5] + v2[5],     #helpfulVotes
        v1[6] + v2[6]      #totalVotes
    )


#mapper job2
def compute_score_and_prepare_ranking(data):
    parent, metrics = data
    title, category, count, sum_rating, sum_ver, sum_help, sum_tot = metrics
    
    avg_rating = sum_rating / count 
    score = avg_rating * math.log10(1 + count) * (1 + (sum_help / (1 + sum_tot)))
    
    #new key
    return (category, (score, parent, title, count, avg_rating, sum_ver, sum_help, sum_tot))


#come reducer job2 - ordiniamo i prodotti 
def apply_ranking(category_data):
    category, products = category_data
    sorted_products = sorted(products, key=lambda x: x[0], reverse=True) #ordiniamo
    
    results = []
    rank = 1
    for p in sorted_products: #facciamo rank
        score, parent, title, count, avg_rating, sum_ver, sum_help, sum_tot = p
        results.append((parent, (title, category, score, rank, count, avg_rating, sum_ver, sum_help, sum_tot)))
        rank += 1
        
    return results


#mapper job3
def detect_anomalies(data):
    parent, metrics = data
    title, category, score, rank, count, avg_rating, ver, help, tot = metrics 
    
    helpful_ratio = help / (1 + tot)
    verified_ratio = ver / count if count > 0 else 0
    
    #thresholds
    min_helpful_ratio = 0.2
    min_verified_ratio = 0.5
    min_reviews_perfect = 100
    
    anomalies = []
    
    #cerco anomalie
    if score > 20 and helpful_ratio < min_helpful_ratio:
        anomalies.append(f"{parent}\tHELPFUL_RATIO_ANOMALY###{title}###{category}###{score}")
        
    if verified_ratio < min_verified_ratio and count > 100:
        anomalies.append(f"{parent}\tVERIFIED_RATIO_ANOMALY###{title}###{category}###{score}")
        
    if avg_rating == 4.9 and count > min_reviews_perfect:
        anomalies.append(f"{parent}\tSUSPICIOUS_PERFECT_PRODUCT###{title}###{category}###{score}")
        
    if rank <= 10 and avg_rating < 3.5:
        anomalies.append(f"{parent}\tRANK_INCONSISTENCY###{title}###{category}###{score}")
        
    return anomalies


def format_ranking_output(data):
    parent, metrics = data
    title, category, score, rank = metrics[0:4]
    return f"{parent}\t{title}###{category}###{score}###{rank}"



if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso corretto: spark-submit amazon_spark.py <input_dataset> <output_ranking> <output_anomalies>")
        sys.exit(-1)

    conf = SparkConf().setAppName("AmazonAnalytics_Spark")
    sc = SparkContext(conf=conf)

    input_path = sys.argv[1]
    out_ranking = sys.argv[2]
    out_anomalies = sys.argv[3]

    raw_rdd = sc.textFile(input_path)
    
    header = raw_rdd.first() 
    clean_rdd = raw_rdd.filter(lambda line: line != header and len(line.split("\t")) >= 9)

    aggregated_rdd = clean_rdd.map(parse_line).reduceByKey(aggregate_metrics)
    ranked_rdd = aggregated_rdd.map(compute_score_and_prepare_ranking).groupByKey().flatMap(apply_ranking)
    ranked_rdd.cache()
    ranked_rdd.map(format_ranking_output).saveAsTextFile(out_ranking)
    anomalies_rdd = ranked_rdd.flatMap(detect_anomalies)
    anomalies_rdd.saveAsTextFile(out_anomalies)

    sc.stop()