from connector_manager.manager.cryptography import decrypt_value
from spotifyconnector import SpotifyConnector


def check_for_new_waiting_list_entries(db, encryption_key):
    sql = """
      SELECT env_name, env_value, value_encrypted, session_id, inserted_at
      FROM podcastConnectWaitlist
      WHERE inserted_at > DATE_SUB(NOW(), INTERVAL 36 HOUR)
    """

    with db.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()

    sessions = {}

    for (env_name, env_value, value_encrypted, session_id, inserted_at) in results:
        if value_encrypted:
            env_value = decrypt_value(env_value, encryption_key)

        if session_id not in sessions:
            sessions[session_id] = {
                "session_id": session_id,
                "inserted_at": inserted_at,
            }

        sessions[session_id][env_name] = env_value

    # for each session, create a new spotify connector and list all shows
    for session_id, session in sessions.items():
        print(f"Processing session {session_id}")
        connector = SpotifyConnector(session)
        catalog = connector.list_shows()

        print(f"Session {session_id} inserted at {session['inserted_at']}")

        for show in catalog["shows"]:
            print(f"Show: {show['name']} with id {show['id']}")

        print("\n")
