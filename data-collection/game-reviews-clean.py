import pymongo
from datetime import datetime, timezone
from langdetect import detect
import re

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["steam_reviews"]
collection = db["reviews"]

# Query data
data = collection.find()

# Get total number of reviews
total_reviews = collection.count_documents({})

# Initialize list to store cleaned data
cleaned_data = []

# Initialize counter for progress tracking
processed_reviews = 0

# Iterate over reviews in the collection
for review in data:
    # Detect the language of the review
    if "review" in review:
        review_text = review["review"]
        try:
            lang = detect(review_text)
        except:
            lang = "unknown"
        
        # Check if the language is English and the review has more than 5 words
        if lang == "en" and len(re.findall(r'\w+', review_text)) >= 5:
            # Clean review text field
            cleaned_review = {}

            cleaned_review["_id"] = review["_id"]
            cleaned_review["recommendationid"] = review["recommendationid"]

            # Clean author field
            if "author" in review:
                author_info = review["author"]
                # Check if "last_played" field exists in the "author" object
                if "last_played" in author_info:
                    last_played_timestamp = author_info["last_played"]
                    author_info["last_played"] = datetime.fromtimestamp(last_played_timestamp, tz=timezone.utc)
                cleaned_review["author"] = author_info
            
            cleaned_review["language"] = review.get("language", "")

            cleaned_review["review"] = review_text

            # Convert Unix timestamps to datetime objects (UTC timezone)
            if "timestamp_created" in review:
                timestamp_created = review["timestamp_created"]
                cleaned_review["timestamp_created"] = datetime.fromtimestamp(timestamp_created, tz=timezone.utc)

            if "timestamp_updated" in review:
                timestamp_updated = review["timestamp_updated"]
                cleaned_review["timestamp_updated"] = datetime.fromtimestamp(timestamp_updated, tz=timezone.utc)
            
            cleaned_review["voted_up"] = review.get("voted_up", False)
            cleaned_review["votes_up"] = review.get("votes_up", 0)
            cleaned_review["votes_funny"] = review.get("votes_funny", 0)
            cleaned_review["weighted_vote_score"] = review.get("weighted_vote_score", "0")
            cleaned_review["comment_count"] = review.get("comment_count", 0)
            cleaned_review["steam_purchase"] = review.get("steam_purchase", False)
            cleaned_review["received_for_free"] = review.get("received_for_free", False)
            cleaned_review["written_during_early_access"] = review.get("written_during_early_access", False)
            cleaned_review["hidden_in_steam_china"] = review.get("hidden_in_steam_china", False)
            cleaned_review["steam_china_location"] = review.get("steam_china_location", "")

            # Add cleaned review to list
            cleaned_data.append(cleaned_review)

    # Increment progress counter
    processed_reviews += 1

    # Print progress every 5000 reviews processed
    if processed_reviews % 5000 == 0:
        print(f"Processed {processed_reviews} out of {total_reviews} reviews...")

# Insert cleaned data into a new MongoDB collection
if cleaned_data:
    cleaned_collection = db["cleaned_reviews"]
    cleaned_collection.insert_many(cleaned_data)
    print("Processing completed!")
else:
    print("No English reviews with more than 5 words found.")
