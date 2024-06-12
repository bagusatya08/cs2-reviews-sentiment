import requests
import urllib.parse
import logging
import pymongo

logging.basicConfig(level=logging.INFO)

def get_steam_reviews(app_id, day_range="30", cursor="*", review_type="all", purchase_type="steam", num_per_page="20", filter_offtopic_activity=1, review_filter="recent"):
    try:
        url = f"https://store.steampowered.com/appreviews/{app_id}?json=1&language=english&day_range={day_range}&cursor={urllib.parse.quote(cursor)}&review_type={review_type}&purchase_type={purchase_type}&num_per_page={num_per_page}&filter_offtopic_activity={filter_offtopic_activity}&filter={review_filter}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch Steam reviews: {e}")
        return None

def connect_to_mongodb():
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        return client
    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return None

def save_reviews_to_mongodb(client, reviews, database_name="steam_reviews", collection_name="reviews"):
    try:
        db = client[database_name]
        collection = db[collection_name]
        collection.insert_many(reviews.get("reviews", []))
        logging.info("Reviews saved to MongoDB.")
    except Exception as e:
        logging.error(f"Failed to save reviews to MongoDB: {e}")

def fetch_and_save_reviews(app_id, reviews_limit=50000, batch_size=100):
    reviews_count = 0
    cursor = "*"
    client = connect_to_mongodb()
    if not client:
        return

    while reviews_count < reviews_limit:
        reviews = get_steam_reviews(app_id, review_filter="recent", cursor=cursor)
        if not reviews:
            logging.warning("No more reviews available.")
            break
        
        unique_reviews_list = []
        
        for review in reviews.get("reviews", []):
            if len(unique_reviews_list) >= batch_size:
                break
            
            if review["recommendationid"] not in {r["recommendationid"] for r in unique_reviews_list}:
                unique_reviews_list.append(review)
                reviews_count += 1
                if reviews_count % batch_size == 0:
                    logging.info(f"Total reviews fetched: {reviews_count}")
        
        cursor = reviews["cursor"]
        
        save_reviews_to_mongodb(client, {"reviews": unique_reviews_list})
        
        if reviews_count >= reviews_limit:
            logging.info("Reviews limit reached.")
            break
        
        if not cursor:
            logging.info("No more reviews available.")

def main():
    try:
        app_id = "730"
        fetch_and_save_reviews(app_id)
    except KeyboardInterrupt:
        logging.warning("Program interrupted by user")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
