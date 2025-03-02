import sys
import os
import grpc
from concurrent import futures
import random
import requests
import pandas as pd
import logging
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import dotenv

# Load environment variables from a .env file (e.g., Google API key)
dotenv.load_dotenv()

# Logging setup for debugging and information
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# gRPC imports for Suggestion Service
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestion_system_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestion_system'))
sys.path.insert(0, suggestion_system_grpc_path)

# Import generated gRPC Python files for Suggestion Service
import suggestion_system_pb2 as suggestions
import suggestion_system_pb2_grpc as suggestions_grpc

# Define the BookRecommender class to handle book recommendation logic
class BookRecommender:
    def __init__(self, books_df, api_key=None):
        # Initialize with a DataFrame containing book data and an optional API key for Google Books
        self.books_df = books_df
        self.api_key = api_key
        self.tfidf_vectorizer = TfidfVectorizer(stop_words='english')  # Vectorizer to compute TF-IDF features
        self._prepare_data()  # Preprocess the data (compute cosine similarity between books)
        
    def _prepare_data(self):
        # Combine genre and description into one feature for better recommendation
        self.books_df['combined_features'] = self.books_df['genre'] + ' ' + self.books_df['description']
        # Compute the TF-IDF matrix for combined features
        self.tfidf_matrix = self.tfidf_vectorizer.fit_transform(self.books_df['combined_features'])
        # Compute cosine similarity between books based on TF-IDF features
        self.cosine_sim = cosine_similarity(self.tfidf_matrix, self.tfidf_matrix)
        logger.info("BookRecommender: Prepared TF-IDF matrix and computed cosine similarity.")
        
    def _get_google_books(self, query, max_results=5):
        # Call Google Books API to get books based on genre
        if not self.api_key:
            logger.warning("Google API key not provided, skipping external API call.")
            return pd.DataFrame()
            
        url = "https://www.googleapis.com/books/v1/volumes"
        params = {
            'q': query,
            'key': self.api_key,
            'maxResults': max_results
        }
        try:
            response = requests.get(url, params=params, timeout=5)  # Make API request
            response.raise_for_status()
            results = []
            for item in response.json().get('items', []):
                volume_info = item.get('volumeInfo', {})
                results.append({
                    'title': volume_info.get('title', 'Unknown Title'),
                    'genre': ', '.join(volume_info.get('categories', ['Unknown Genre'])),
                    'description': volume_info.get('description', 'No description available')
                })
            logger.info("Google Books API call successful.")
            return pd.DataFrame(results)  # Return the book results as a DataFrame
        except Exception as e:
            logger.error(f"Google Books API error: {e}")
            return pd.DataFrame()  # Return empty DataFrame if error occurs

    def get_recommendations(self, book_title, top_n=5):
        try:
            # Find the index of the given book in the DataFrame
            idx_list = self.books_df.index[self.books_df['title'] == book_title].tolist()
            if not idx_list:
                logger.warning(f"Book '{book_title}' not found in local database.")
                return pd.DataFrame()  # Return empty DataFrame if the book is not found
                
            idx = idx_list[0]
            # Get cosine similarity scores for all books with respect to the given book
            sim_scores = list(enumerate(self.cosine_sim[idx]))
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)  # Sort by similarity
            top_indices = [i[0] for i in sim_scores[1:top_n+1]]  # Get the top N similar books
            local_recs = self.books_df.iloc[top_indices]  # Get the top N books from local DataFrame

            # Use the genre from the local book to get API-based recommendations
            genre = self.books_df.iloc[idx]['genre']
            api_recs = self._get_google_books(f'subject:{genre}')
            
            # Combine local recommendations with API-based recommendations
            combined = pd.concat([local_recs, api_recs], ignore_index=True)
            combined.drop_duplicates(subset=['title'], inplace=True)  # Remove duplicates by title
            recommendations = combined.head(top_n * 2)  # Limit to top N * 2 to account for duplicates
            logger.info(f"Found {len(recommendations)} recommendations for '{book_title}'.")
            return recommendations
        except Exception as e:
            logger.error(f"Error generating recommendations for '{book_title}': {e}")
            return pd.DataFrame()  # Return empty DataFrame in case of error

# Define the gRPC service that serves book suggestions
class SuggestionService(suggestions_grpc.SuggestionServiceServicer):
    def __init__(self):
        # Initialize local book dataset with some sample books
        self.books_df = pd.DataFrame({
            'title': ['The Hobbit', '1984', 'The Lord of the Rings'],
            'genre': ['Fantasy', 'Dystopian', 'Fantasy'],
            'description': [
                'A hobbit joins a quest to reclaim a dwarf kingdom.',
                'A dystopian novel about a totalitarian society.',
                'An epic fantasy adventure to destroy a powerful ring.'
            ]
        })
        # Initialize the recommender with the local dataset and optional Google API key
        self.recommender = BookRecommender(
            self.books_df,
            api_key=os.getenv("GOOGLE_API_KEY")
        )
        logger.info("SuggestionService initialized with local book data.")

    def GetSuggestions(self, request, context):
        # Handle the incoming gRPC request to get book suggestions
        logger.info(f"Received GetSuggestions request for book title: {request.book_title}")
        response = suggestions.SuggestionResponse()
        
        try:
            # Get book recommendations using the BookRecommender class
            recommendations = self.recommender.get_recommendations(request.book_title)
            if not recommendations.empty:
                for _, row in recommendations.iterrows():
                    response.suggestions.append(
                        suggestions.BookSuggestion(
                            title=row['title'],
                            genre=row.get('genre', 'Unknown Genre'),
                            description=row.get('description', 'No description available')
                        )
                    )
            else:
                # If no recommendations found, fallback to random suggestions from the local dataset
                logger.info("No recommendations found; using fallback suggestions.")
                fallback_count = min(3, len(self.books_df))  # Limit to 3 fallback suggestions
                for book in random.sample(self.books_df.to_dict('records'), fallback_count):
                    response.suggestions.append(
                        suggestions.BookSuggestion(
                            title=book['title'],
                            genre=book['genre'],
                            description=book['description']
                        )
                    )
        except Exception as e:
            # Handle any errors that occur during recommendation generation
            logger.error(f"Error in GetSuggestions: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Error generating suggestions: {str(e)}")
            
        return response

# Define the gRPC server and serve the suggestions service
def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    suggestions_grpc.add_SuggestionServiceServicer_to_server(SuggestionService(), server)
    port = "50053"
    server.add_insecure_port("[::]:" + port)  # Bind server to the specified port
    server.start()  # Start the gRPC server
    logger.info(f"Suggestions service started. Listening on port {port}.")
    server.wait_for_termination()  # Keep the server running indefinitely

# Start the gRPC server if the script is executed
if __name__ == '__main__':
    serve()
