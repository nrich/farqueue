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

#include <time.h>
#include <syslog.h>

/* gcc -D_GNU_SOURCE -pedantic -o farqueue farqueue.c -levent -lsqlite3 */

static int cleanup_timeout = 60;

static sqlite3 *sqlite = NULL;
static sqlite3_stmt *dq = NULL;
static sqlite3_stmt *nq = NULL;
static sqlite3_stmt *del = NULL;
static sqlite3_stmt *stats = NULL;
static sqlite3_stmt *cln = NULL;

#define TEMP_QUEUE_NAME "%$TEMP$%"

#define DQ_QUERY  "SELECT id,data,reply FROM queues WHERE name=?  AND (timeout IS NULL OR (CAST(strftime('%s', created) AS INTEGER) + timeout) >= CAST(strftime('%s', CURRENT_TIMESTAMP) AS INTEGER)) ORDER BY priority DESC,id"
#define NQ_QUERY  "INSERT INTO queues(name, data, priority, reply, timeout) VALUES(?,?,?,?,?)"
#define DEL_QUERY "DELETE FROM queues WHERE name=? AND id=?"
#define CREATE_QUERY    "CREATE TABLE IF NOT EXISTS queues(" \
                        "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,"\
			"created INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,"\
			"timeout INTEGER,"\
			"reply VARCHAR(254)," \
			"priority INTEGER NOT NULL DEFAULT 0," \
                        "name VARCHAR(254) NOT NULL," \
                        "data text)"
#define STATS_QUERY  "SELECT name,count(1),strftime('%s', min(created)), strftime('%s', max(created)) FROM queues WHERE NAME NOT LIKE ? GROUP BY name"
#define CLN_QUERY   "DELETE FROM queues WHERE timeout IS NOT NULL AND (CAST(strftime('%s', created) AS INTEGER) + timeout) < CAST(strftime('%s', CURRENT_TIMESTAMP) AS INTEGER)"

#ifdef DEBUG
void debug(char *format, ...) {
    va_list             argptr;
    char                text[4094];

    va_start (argptr, format);
    vsprintf (text, format, argptr);
    va_end (argptr);

    puts(text);
}
#endif

void init_persitance(char *db) {
    /* TODO catch errors from sqlite properly */ 

    if (sqlite3_open(db, &sqlite) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to open DB: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    if (sqlite3_exec(sqlite, CREATE_QUERY, NULL, NULL, NULL) != SQLITE_OK) {
	syslog(LOG_ERR, "Cannot create queue table: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    if (sqlite3_prepare_v2(sqlite, DQ_QUERY, strlen(DQ_QUERY), &dq, NULL) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to prepare dequeue query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    if (sqlite3_prepare_v2(sqlite, NQ_QUERY, strlen(NQ_QUERY), &nq, NULL) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to prepare enqueue query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    if (sqlite3_prepare_v2(sqlite, DEL_QUERY, strlen(DEL_QUERY), &del, NULL) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to prepare delete query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    if (sqlite3_prepare_v2(sqlite, STATS_QUERY, strlen(STATS_QUERY), &stats, NULL) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to prepare stats query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    if (sqlite3_prepare_v2(sqlite, CLN_QUERY, strlen(CLN_QUERY), &cln, NULL) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to prepare cleanup query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }
}

void cleanup(int fd, short event, void *arg) {
    struct event *ev = arg;

    struct timeval tv;

    sqlite3_reset(cln);
    if (sqlite3_step(cln) != SQLITE_DONE) {
	syslog(LOG_WARNING, "Cleanup failed %s\n", sqlite3_errmsg(sqlite)); 
    } else {
	int affected = sqlite3_changes(sqlite);

	if (affected)
	    syslog(LOG_INFO, "Cleaned %d rows", affected);
    }

    timerclear(&tv);
    tv.tv_sec = cleanup_timeout;

    event_add(ev, &tv);
}

void dequeue(const char *queue_name, struct evhttp_request *req, struct evbuffer *buf) {
    int res;

#ifdef DEBUG
    debug("DQ %s\n", queue_name);
#endif

    if (sqlite3_reset(dq) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to reset dequeue query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    if (sqlite3_reset(del) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to reset delete query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    sqlite3_bind_text(dq, 1, queue_name, -1, SQLITE_STATIC);
    res = sqlite3_step(dq);
    
    if (res == SQLITE_ROW) {
        int id = sqlite3_column_int(dq, 0);
        const char *val = (const char *)sqlite3_column_text(dq, 1);
	const char *reply = (const char *)sqlite3_column_text(dq, 2);

	if (reply) {
	    evhttp_add_header(req->output_headers, "queue-reply", reply);
	}

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
    int priority = 0;
    const char *reply;

#ifdef DEBUG
    debug("NQ %s\n", queue_name);
#endif

    if (sqlite3_reset(nq) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to reset enqueue query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    pdata = EVBUFFER_DATA(req->input_buffer);

    if (!pdata) {
        evbuffer_add_printf(buf, "Bad Request");
        evhttp_send_reply(req, 400, "Bad Request", buf);
        return;
    }

    {
	const char *queue_priority = evhttp_find_header(req->input_headers, "queue-priority");
	if (queue_priority) {
	    priority = atoi(queue_priority);
	}
    }

    reply = evhttp_find_header(req->input_headers, "queue-reply");

    data = strndup((char *)pdata, (size_t)EVBUFFER_LENGTH(req->input_buffer));

    sqlite3_bind_text(nq, 1, queue_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(nq, 2, evhttp_decode_uri(data), -1, SQLITE_STATIC);
    sqlite3_bind_int(nq, 3, priority);

    if (reply)
	sqlite3_bind_text(nq, 4, reply, -1, SQLITE_STATIC);
    else
	sqlite3_bind_null(nq, 4);

    {
	const char *queue_timeout = evhttp_find_header(req->input_headers, "queue-timeout");

	if (queue_timeout) {
	    int timeout = atoi(queue_timeout);

	    if (timeout > 0) {
		sqlite3_bind_int(nq, 5, timeout);
	    } else {
		sqlite3_bind_null(nq, 5);
	    }
	} else {
	    sqlite3_bind_null(nq, 5);
	}
    }

    sqlite3_step(nq);

    free(data);

    evbuffer_add_printf(buf, "{\"msg\":\"OK\"}", queue_name);
    evhttp_send_reply(req, HTTP_OK, "OK", buf);
}

void stats_handler(struct evhttp_request *req, void *arg) {
    int res;
    int rows = 0;

    struct evbuffer *buf;
    buf = evbuffer_new();

    if (buf == NULL) {
	syslog(LOG_ERR, "Unable to allocate response buffer for stats request");
	exit(-1);
    }

    
    if (sqlite3_reset(stats) != SQLITE_OK) {
	syslog(LOG_ERR, "Unable to reset stats query: %s\n", sqlite3_errmsg(sqlite));    
	exit(-1);
    }

    sqlite3_bind_text(stats, 1, TEMP_QUEUE_NAME, -1, SQLITE_STATIC);

    evbuffer_add_printf(buf, "[");

    while (sqlite3_step(stats) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stats, 0);
        int count = sqlite3_column_int(stats, 1);
        int min = sqlite3_column_int(stats, 2);
        int max = sqlite3_column_int(stats, 3);

	if (rows)
	    evbuffer_add_printf(buf, ",");

        evbuffer_add_printf(buf, "{\"name\":\"%s\",\"count\":%d,\"min\":%d,\"max\":%d}", name, count, min, max);
	rows++;
    }

    evbuffer_add_printf(buf, "]");
    evhttp_send_reply(req, HTTP_OK, "OK", buf);
}

void generic_handler(struct evhttp_request *req, void *arg) {
    const char *queue_name;

    struct evbuffer *buf;
    buf = evbuffer_new();

    if (buf == NULL) {
	syslog(LOG_ERR, "Unable to allocate response buffer");
	exit(-1);
    }

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
    struct event ev;
    struct evhttp *httpd;
    char *db = "/tmp/farqueue.db";
    char *host = "127.0.0.1";
    int port = 9094;
    int c;
    int daemonise = 0;
    int option = LOG_PID;

    struct timeval tv;


    opterr = 0;

    while ((c = getopt(argc, argv, "h:p:f:c:d")) != -1) {
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
	case 'c':
	    cleanup_timeout = atoi(optarg);
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
    } else {
	option |= LOG_PERROR;
    }

    openlog("farqueue", option, LOG_USER);

    init_persitance(db); 

    event_init();
    httpd = evhttp_start(host, port);

    /* Set a callback for requests to "/specific". */
    /* evhttp_set_cb(httpd, "/specific", another_handler, NULL); */

    evhttp_set_cb(httpd, "/$STATS$", stats_handler, NULL);

    /* Set a callback for all other requests. */
    evhttp_set_gencb(httpd, generic_handler, NULL);

    if (cleanup_timeout > 0) {
	event_set(&ev, -1, EV_TIMEOUT, cleanup, &ev);
	timerclear(&tv);
	tv.tv_sec = cleanup_timeout;
	event_add(&ev, &tv);
    }

    event_dispatch();

    /* Not reached in this code as it is now. */
    evhttp_free(httpd);

    return 0;
}
