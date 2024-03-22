from flask import Flask, request, jsonify
from queue import Queue  # Import Queue for message queuing
from index1 import app, cursor, connection
fapp = Flask(__name__)
query_queue = Queue()  # Create a thread-safe queue for incoming messages

def process_query(query):
 
    try:
        result = app.invoke({"message": query})
        print(result["final_sql_query"].sql_query)

        cursor.execute(result["final_sql_query"].sql_query)
        connection.commit()

    except Exception as e:
        print("Error processing query:", e)

# ... your other Flask app routes (e.g., /query)

@fapp.route('/query', methods=['POST'])
def handle_query():
 
    try:
        data = request.get_json()
        query = data.get('query')

        # Add the query to the queue for asynchronous processing
        query_queue.put(query)

        return jsonify("Query queued successfully.")

    except Exception as e:
        print("Error handling query request:", e)
        return jsonify("Error: Could not queue query.")

if __name__ == '__main__':
    # ... (Your existing code for starting the Flask app)

    # Start a separate thread to process queries from the queue asynchronously
    from threading import Thread
    def worker():
        while True:
            query = query_queue.get()  # Block until a new query is available
            process_query(query)
            query_queue.task_done()  # Mark the task as completed

    worker_thread = Thread(target=worker)
    worker_thread.daemon = True  # Make the worker thread a daemon
    worker_thread.start()

    fapp.run(port=5000)



