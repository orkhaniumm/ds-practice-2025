import sys
import os
import grpc
from concurrent import futures
import random
import requests
import logging
import json

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure gRPC stub paths
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestions_grpc_path)

import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

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
        params = {
            "q": q,
            "maxResults": desired_count * 2  # Additional results for filtering out duplicates
        }
        logger.info(f"Sending Google Books API request with query: '{q}' and params: {params}")
        try:
            response = requests.get(base_url, params=params, timeout=5) # Fetch data
            response.raise_for_status()
            data = response.json()
            logger.info(f"Received response from Google Books API: {json.dumps(data, indent=2)[:500]}...")
            for item in data.get("items", []):
                volume_info = item.get("volumeInfo", {})
                title = volume_info.get("title", "Unknown Title")
                authors = volume_info.get("authors")
                author = authors[0] if authors and isinstance(authors, list) and len(authors) > 0 else "Unknown Author"
                collected.append({
                    "title": title,
                    "author": author
                })
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
    logger.info(f"Final collected suggestions: {final_list}")
    return final_list

class SuggestionService(suggestions_grpc.SuggestionServiceServicer):
    def __init__(self):
        # Initialize with static default data.
        self.books = STATIC_BOOKS
        logger.info("SuggestionService initialized with static default book data.")

    def GetSuggestions(self, request, context):
        logger.info(f"Received GetSuggestions request for book title: {request.book_title}")
        response = suggestions.SuggestionResponse()
        
        # Try Google Books API first.
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
        
        # Build the response with only title and author.
        for suggestion in suggestions_list:
            response.suggestions.append(
                suggestions.BookSuggestion(
                    title=suggestion.get("title", "Unknown Title"),
                    author=suggestion.get("author", "Unknown Author")
                )
            )
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    suggestions_grpc.add_SuggestionServiceServicer_to_server(SuggestionService(), server)
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(f"Suggestions service started. Listening on port {port}.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
