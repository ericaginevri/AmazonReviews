import sys
import math
import time

if __name__ == "__main__":
    #check input
    if len(sys.argv) < 2:
        print(
            "Usage: python3 amazon_local.py <input_file>",
            file=sys.stderr
        )
        sys.exit(-1)

    input_file = sys.argv[1]
    start_time = time.time()
    products = {}

    #read dataset
    with open(input_file, "r", encoding="utf-8") as f:
        #skip header
        next(f)
        for line in f:
            fields = line.split("\t")
            if len(fields) < 9:
                continue

            #extract fields
            product_parent = fields[2]
            product_title = fields[3]
            category = fields[4]
            rating = float(fields[5])
            verified = (
                1 if fields[8] == "Y" else 0
            )
            helpful_votes = int(fields[6])
            total_votes = int(fields[7])

            #aggregation
            if product_parent not in products:
                products[product_parent] = {
                    "title": product_title,
                    "category": category,
                    "review_count": 0,
                    "rating_sum": 0.0,
                    "verified_count": 0,
                    "helpful_votes": 0,
                    "total_votes": 0
                }

            products[product_parent]["review_count"] += 1
            products[product_parent]["rating_sum"] += rating
            products[product_parent]["verified_count"] += verified
            products[product_parent]["helpful_votes"] += helpful_votes
            products[product_parent]["total_votes"] += total_votes

    #compute score
    ranking = []
    
    for product_parent, data in products.items():
        avg_rating = (
            data["rating_sum"] /
            data["review_count"]
        )

        helpful_ratio = (
            data["helpful_votes"] /
            (1 + data["total_votes"])
        )

        verified_ratio = (
            data["verified_count"] /
            data["review_count"]
        )

        score = (
            avg_rating *
            math.log(1 + data["review_count"]) *
            (1 + helpful_ratio)
        )
        
        data["avg_rating"] = avg_rating
        data["helpful_ratio"] = helpful_ratio
        data["verified_ratio"] = verified_ratio
        data["score"] = score

        ranking.append(
            (
                product_parent,
                data["title"],
                data["category"],
                score
            )
        )

    #sort ranking
    ranking.sort(
        key=lambda x: x[3],
        reverse=True
    )

    #anomaly detection 
    anomalies = []
    
    for index, p in enumerate(ranking):
        rank_position = index + 1
        product_parent = p[0]
        score = p[3]
        data = products[product_parent]
        
        avg_rating = data["avg_rating"]
        helpful_ratio = data["helpful_ratio"]
        verified_ratio = data["verified_ratio"]
        review_count = data["review_count"]

        if score > 20 and helpful_ratio < 0.2:
            anomalies.append(
                (
                    product_parent,
                    "HELPFUL_RATIO_ANOMALY",
                    score
                )
            )

        if verified_ratio < 0.5 and review_count > 100:
            anomalies.append(
                (
                    product_parent,
                    "VERIFIED_RATIO_ANOMALY",
                    score
                )
            )

        if avg_rating > 4.9 and review_count > 100:
            anomalies.append(
                (
                    product_parent,
                    "SUSPICIOUS_PERFECT_PRODUCT",
                    score
                )
            )
        
        if rank_position <= 10 and avg_rating < 3.5:
            anomalies.append(
                (
                    product_parent,
                    "RANK_INCONSISTENCY",
                    score
                )
            )


    end_time = time.time()

    #statistics
    print("\nExecution statistics")
    print(
        f"Execution time: "
        f"{end_time - start_time:.2f} sec"
    )
    print(
        f"Products processed: "
        f"{len(products)}"
    )
    print(
        f"Anomalies found: "
        f"{len(anomalies)}"
    )
    print("\nTop 10 products")

    for p in ranking[:10]:
        print(
            f"{p[0]} | "
            f"{p[1]} | "
            f"{p[2]} | "
            f"{p[3]:.2f}"
        )

    print("\nFirst 10 anomalies")

    for a in anomalies[:10]:
        print(a)