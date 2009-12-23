#include <sys/types.h>
#include <sys/time.h>
#include <sys/queue.h>
#include <stdlib.h>

#include <string.h>
#include <stdio.h>

#include <err.h>
#include <event.h>
#include <evhttp.h>

#include <sqlite3.h>

#include <unistd.h>


/* gcc -D_GNU_SOURCE -pedantic -o farqueue farqueue.c -levent -lsqlite3 */

static sqlite3 *sqlite = NULL;
static sqlite3_stmt *dq = NULL;
static sqlite3_stmt *nq = NULL;
static sqlite3_stmt *del = NULL;

#define DQ_QUERY  "SELECT id,data FROM queues WHERE name=? ORDER BY id"
#define NQ_QUERY  "INSERT INTO queues(name, data) VALUES(?,?)"
#define DEL_QUERY "DELETE FROM queues WHERE name=? AND id=?"
#define CREATE_QUERY    "CREATE TABLE IF NOT EXISTS queues(" \
                        "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"\
                        "name VARCHAR(254) NOT NULL," \
                        "data text)"

void init_persitance(char *db) {
    /* TODO catch errors from sqlite properly */ 

    if (sqlite3_open(db, &sqlite) != SQLITE_OK) {
        err(1, "Unable to open DB");
    }

    if (sqlite3_exec(sqlite, CREATE_QUERY, NULL, NULL, NULL) != SQLITE_OK) {
        err(1, "Cannot create queue table");
    }

    if (sqlite3_prepare_v2(sqlite, DQ_QUERY, strlen(DQ_QUERY), &dq, NULL) != SQLITE_OK) {
        err(1, "Unable to prepare dequeue query");
    }

    if (sqlite3_prepare_v2(sqlite, NQ_QUERY, strlen(NQ_QUERY), &nq, NULL) != SQLITE_OK) {
        err(1, "Unable to prepare enqueue query");
    }

    if (sqlite3_prepare_v2(sqlite, DEL_QUERY, strlen(DEL_QUERY), &del, NULL) != SQLITE_OK) {
        err(1, "Unable to prepare delete query");
    }

}

void dequeue(const char *queue_name, struct evhttp_request *req, struct evbuffer *buf) {
    int res;

    if (sqlite3_reset(dq) != SQLITE_OK) {
        err(1, "Unable to reset dequeue query");
    }

    if (sqlite3_reset(del) != SQLITE_OK) {
        err(1, "Unable to reset delete query");
    }

    sqlite3_bind_text(dq, 1, queue_name, -1, SQLITE_STATIC);
    res = sqlite3_step(dq);
    
    if (res == SQLITE_ROW) {
        int id = sqlite3_column_int(dq, 0);
        const char *val = (const char *)sqlite3_column_text(dq, 1);

        sqlite3_bind_text(del, 1, queue_name, -1, SQLITE_STATIC);
        sqlite3_bind_int(del, 2, id);        
        res = sqlite3_step(del);

        evbuffer_add_printf(buf, "%s", val);
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
    } else {
        evbuffer_add_printf(buf, "null");
        evhttp_send_reply(req, HTTP_NOTFOUND, "Empty", buf);
    }
}

void enqueue(const char *queue_name, struct evhttp_request *req, struct evbuffer *buf) {
    char *data;
    unsigned const char *pdata;

    if (sqlite3_reset(nq) != SQLITE_OK) {
        err(1, "Unable to reset enqueue query");
    }

    pdata = EVBUFFER_DATA(req->input_buffer);

    if (!pdata) {
        evbuffer_add_printf(buf, "Bad Request");
        evhttp_send_reply(req, 400, "Bad Request", buf);
        return;
    }

    data = strndup((char *)pdata, (size_t)EVBUFFER_LENGTH(req->input_buffer));

    sqlite3_bind_text(nq, 1, queue_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(nq, 2, evhttp_decode_uri(data), -1, SQLITE_STATIC);
    sqlite3_step(nq);

    free(data);

    evbuffer_add_printf(buf, "{\"msg\":\"OK\"}", queue_name);
    evhttp_send_reply(req, HTTP_OK, "OK", buf);
}


void generic_handler(struct evhttp_request *req, void *arg) {
    const char *queue_name;

    struct evbuffer *buf;
    buf = evbuffer_new();

    if (buf == NULL)
        err(1, "failed to create response buffer");

    queue_name = evhttp_request_uri(req);
    queue_name++;

    if (!strlen(queue_name)) {
        evbuffer_add_printf(buf, "Queue '%s' not found", queue_name);
        evhttp_send_reply(req, HTTP_NOTFOUND, "Queue not found", buf);
        return;
    }    

    if (req->type == EVHTTP_REQ_GET) {
        dequeue(queue_name, req, buf);
    } else if (req->type == EVHTTP_REQ_POST) {
        enqueue(queue_name, req, buf);
    }

    evbuffer_free(buf);
}

int main(int argc, char **argv) {
    struct evhttp *httpd;
    char *db = "/tmp/farqueue.db";
    char *host = "127.0.0.1";
    int port = 9094;
    int c;
    int daemonise = 0;

    opterr = 0;

    while ((c = getopt(argc, argv, "h:p:f:d")) != -1) {
        switch (c) {
        case 'h':
            host = optarg;
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'f':
            db = optarg;
            break;
        case 'd':
            daemonise = 1;
            break;
        default:
             abort();
        }
    }

    if (daemonise) {
        int child;

        if (child = fork()) {
            fprintf(stdout, "%d\n", child);
            exit(0);
        } else {
        }
    }

    init_persitance(db); 

    event_init();
    httpd = evhttp_start(host, port);

    /* Set a callback for requests to "/specific". */
    /* evhttp_set_cb(httpd, "/specific", another_handler, NULL); */

    /* Set a callback for all other requests. */
    evhttp_set_gencb(httpd, generic_handler, NULL);

    event_dispatch();

    /* Not reached in this code as it is now. */
    evhttp_free(httpd);

    return 0;
}
