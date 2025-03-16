import sys
import os
import grpc
import json
import logging
import random
import requests
from concurrent import futures

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub paths
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestions_grpc_path)

import suggestions_pb2 as sugg_pb2
import suggestions_pb2_grpc as sugg_grpc

# Default static book data - author and title.
STATIC_BOOKS = [
    {"title": "The Hobbit", "author": "J.R.R. Tolkien"},
    {"title": "1984", "author": "George Orwell"},
    {"title": "The Lord of the Rings", "author": "J.R.R. Tolkien"},
    {"title": "Dune", "author": "Frank Herbert"},
    {"title": "Brave New World", "author": "Aldous Huxley"}
]

def get_suggestions_google_books(book_title, desired_count=3):
    """
    Uses the Google Books API to fetch basic book info for the given book title.
    """
    # Define query params and base URL.
    base_url = "https://www.googleapis.com/books/v1/volumes"
    queries = [f"intitle:{book_title}", book_title]
    collected = []

    for q in queries:
        params = {"q": q, "maxResults": desired_count * 2}
        logger.info(f"Sending Google Books API request with query: '{q}' and params: {params}")
        try:
            response = requests.get(base_url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Received response from Google Books API: {json.dumps(data, indent=2)[:500]}...")
            for item in data.get("items", []):
                volume_info = item.get("volumeInfo", {})
                title = volume_info.get("title", "Unknown Title")
                authors = volume_info.get("authors")
                author = authors[0] if authors and isinstance(authors, list) and len(authors) > 0 else "Unknown Author"
                collected.append({"title": title, "author": author})
            # Remove duplicates based on title.
            unique = {}
            for s in collected:
                unique[s["title"]] = s
            logger.info(f"Unique suggestions collected so far: {list(unique.values())}")
            if len(unique) >= desired_count:
                return list(unique.values())[:desired_count]
        except Exception as e:
            logger.error(f"Google Books API error for query '{q}': {e}")
            continue

    # Return the final list of suggestions.
    final_list = list({s["title"]: s for s in collected}.values())[:desired_count]
    logger.info(f"Final collected suggestions (after merging): {final_list}")
    return final_list

# Merge vector clocks and increment the suggestions index.
def merge_and_increment(local_clock, incoming_clock, idx_name="suggestions"):
    merged = {}
    for key in set(local_clock.keys()).union(incoming_clock.keys()):
        merged[key] = max(local_clock.get(key, 0), incoming_clock.get(key, 0))
    merged[idx_name] = merged.get(idx_name, 0) + 1
    return merged

class SuggestionService(sugg_grpc.SuggestionServiceServicer):
    def __init__(self):
         # Initialize with static default data.
        self.orders = {}  

    # Initalie an order for suggestions by parsing the checkout data.
    def InitOrder(self, request, context):
        logger.info(f"[Suggestions] InitOrder for {request.orderId}")
        try:
            data = json.loads(request.checkoutData)
        except Exception as e:
            logger.error(f"[Suggestions] InitOrder parse error: {e}")
            return sugg_pb2.SuggestionInitResponse(success=False, clock="")
        init_clock = {"transaction": 0, "fraud": 0, "suggestions": 0}
        self.orders[request.orderId] = {"data": data, "clock": init_clock}
        return sugg_pb2.SuggestionInitResponse(success=True, clock=json.dumps(init_clock))

    # Generate suggestions based on the book title.
    def GenerateSuggestions(self, request, context):
        logger.info(f"[Suggestions] GenerateSuggestions for {request.orderId}")
        entry = self.orders.get(request.orderId)
        if not entry:
            return sugg_pb2.SuggestionResponse(suggestions=[], clock=request.clock)
        local_clock = entry["clock"]
        try:
            incoming_clock = json.loads(request.clock) if request.clock else local_clock
        except:
            incoming_clock = local_clock
        merged_clock = merge_and_increment(local_clock, incoming_clock, "suggestions")
        entry["clock"] = merged_clock

        # Try Google Books API first. If it doesn't return enough results, use static data.
        suggestions_list = get_suggestions_google_books(request.book_title)
        if len(suggestions_list) < 3:
            logger.info("Insufficient suggestions from Google Books API; using fallback suggestions.")
            # Ensure exactly 3 suggestions are returned - this is our current standard
            fallback = random.sample(STATIC_BOOKS, min(3, len(STATIC_BOOKS)))
            merged = {s["title"]: s for s in suggestions_list}
            for fb in fallback:
                if len(merged) < 3 and fb["title"] not in merged:
                    merged[fb["title"]] = fb
            suggestions_list = list(merged.values())[:3]
            logger.info(f"Final suggestions after merging fallback: {suggestions_list}")

        response = sugg_pb2.SuggestionResponse()
        # Build the response with only title and author.
        for suggestion in suggestions_list:
            response.suggestions.append(
                sugg_pb2.BookSuggestion(
                    title=suggestion.get("title", "Unknown Title"),
                    author=suggestion.get("author", "Unknown Author")
                )
            )
        response.clock = json.dumps(merged_clock)
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    sugg_grpc.add_SuggestionServiceServicer_to_server(SuggestionService(), server)
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(f"Suggestions service started. Listening on port {port}.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
