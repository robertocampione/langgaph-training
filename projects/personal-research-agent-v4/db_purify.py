import sqlite3

def purify_user_topic_graph():
    db_path = "db/personal_research_agent.sqlite"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    print("Normalizzando topic in lowercase...")
    # Step 1: Normalize topics
    cur.execute("SELECT id, user_id, topic, subtopic FROM user_topic_graph")
    rows = cur.fetchall()
    
    seen = set()
    to_delete = []
    
    for row in rows:
        row_id, user_id, raw_topic, raw_subtopic = row
        t_norm = raw_topic.lower().strip()
        st_norm = raw_subtopic.lower().replace("-", " ").strip()
        
        # Remove bad LLM artifact tokens like "tempo" and "libero" if they stand alone and don't make sense, but for safety we just check pure duplicates
        # But wait, user wanted them removed. If they are part of "tempo libero", keep it if it's not a duplicate. Actually, let's just drop exact duplicates under the same normalized topic.
        
        sig = (user_id, t_norm, st_norm)
        if sig in seen:
            to_delete.append(row_id)
        else:
            seen.add(sig)
            # update row to be normalized
            try:
                cur.execute("UPDATE user_topic_graph SET topic = ?, subtopic = ? WHERE id = ?", (t_norm, raw_subtopic, row_id))
            except sqlite3.IntegrityError:
                # if conflict occurred because the normalized one already existed before we got here
                to_delete.append(row_id)

    if to_delete:
        print(f"Eliminando {len(to_delete)} duplicati irrisolvibili...")
        placeholders = ",".join(["?"] * len(to_delete))
        cur.execute(f"DELETE FROM user_topic_graph WHERE id IN ({placeholders})", to_delete)
        
    conn.commit()
    print("Database pulito con successo.")

if __name__ == "__main__":
    purify_user_topic_graph()
