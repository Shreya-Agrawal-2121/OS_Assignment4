#include <iostream>
#include <bits/stdc++.h>
#include <pthread.h>
#include <ctime>
#include <cmath>
using namespace std;
typedef long long ll;
enum Action
{
    POST,
    COMMENT,
    LIKE
};
typedef struct _user_action
{
    ll user_id;
    ll action_id;
    Action action_type;
    time_t created_time;

} user_action;
typedef struct _node
{
    ll id;
    ll priority;
    ll degree;
    ll action_count_post;
    ll action_count_like;
    ll action_count_comment;
    queue<user_action> wall_queue, feed_queue;

} node;
const ll N = 37700;
vector<ll> adj_list[N];
vector<node> nodes(N);
queue<user_action> shared_queue;
ll **num_mutual_frnds;
ll count_empty_feed = N;
pthread_mutex_t tlock, tcount;
pthread_cond_t tcond, tread;
void init_graph()
{
    ifstream input_file("musae_git_edges.csv");
    string line;
    getline(input_file, line);
    while (getline(input_file, line))
    {
        stringstream linestream(line);
        vector<ll> nodenum;
        string nodestr;
        while (getline(linestream, nodestr, ','))
        {
            nodenum.push_back((ll)atoi(nodestr.c_str()));
        }
        if (nodenum.size() != 2)
        {
            perror("Edge specification incorrect");
            exit(0);
        }
        adj_list[nodenum[0]].push_back(nodenum[1]);
        adj_list[nodenum[1]].push_back(nodenum[0]);
    }
    for (ll i = 0; i < N; i++)
    {
        nodes[i].id = i;
        nodes[i].priority = rand() % 2;
        nodes[i].degree = adj_list[i].size();
        nodes[i].action_count_comment = nodes[i].action_count_like = nodes[i].action_count_post = 0LL;
    }
    num_mutual_frnds = (ll **)malloc(N * sizeof(ll *));
    for (ll i = 0; i < N; i++)
        num_mutual_frnds[i] = (ll *)malloc(N * sizeof(ll));
    for (ll i = 0; i < N; i++)
    {
        for (ll j = 0; j < N; j++)
            num_mutual_frnds[i][j] = -1LL;
    }
}
void calc_mutual_friends(ll i, ll j)
{
    num_mutual_frnds[i][j] = num_mutual_frnds[j][i] = 0LL;
    set<ll> st;
    for (auto it : adj_list[i])
        st.insert(it);
    for (auto it : adj_list[j])
    {
        if (st.find(it) != st.end())
        {
            num_mutual_frnds[i][j]++;
            num_mutual_frnds[j][i]++;
        }
    }
}
void *userSimulator(void *args)
{

    while (1)
    {
        pthread_mutex_lock(&tlock);

        srand(time(0));
        string msg = "";

        for (ll i = 0; i < 100; i++)
        {
            ll node_idx = rand() % N;
            ll no_actions = (ll)floor(log2(nodes[node_idx].degree));
            msg += "Random node : ";
            msg += to_string(node_idx);
            msg += "\n";
            msg += "No of actions generated: ";
            msg += to_string(no_actions);
            msg += "\n";
            msg += "Degree of node: ";
            msg += to_string(nodes[node_idx].degree);
            msg += "\n";
            msg += "Actions generated :";
            for (ll j = 0; j < no_actions; j++)
            {
                ll act_type = rand() % 3;
                user_action new_act;
                new_act.user_id = node_idx;
                new_act.created_time = time(NULL);

                switch (act_type)
                {
                case 0:
                    new_act.action_type = POST;
                    nodes[node_idx].action_count_post++;
                    new_act.action_id = nodes[node_idx].action_count_post;
                    msg += "POST";
                    break;
                case 1:
                    new_act.action_type = COMMENT;
                    nodes[node_idx].action_count_comment++;
                    new_act.action_id = nodes[node_idx].action_count_comment;
                    msg += "COMMENT";
                    break;
                case 2:
                    new_act.action_type = LIKE;
                    nodes[node_idx].action_count_like++;
                    new_act.action_id = nodes[node_idx].action_count_like;
                    msg += "LIKE";
                    break;
                }
                if (j == no_actions - 1)
                    msg += "\n";
                else
                    msg += ", ";
                nodes[node_idx].wall_queue.push(new_act);
                shared_queue.push(new_act);
                pthread_cond_broadcast(&tcond);
            }
        }
        cout << msg << "\n";

        string logFileName = "sns.log";
        ofstream logFile(logFileName);
        if (logFile.is_open())
        {
            // write to the log file
            logFile << msg << endl;

            // close the log file
            logFile.close();
        }
        else
        {
            cout << "Failed to open log file." << endl;
        }
        pthread_mutex_unlock(&tlock);

        sleep(120);
    }
    return NULL;
}

bool sortByTime(user_action a, user_action b)
{
    return difftime(a.created_time, b.created_time) < 0 ? true : false;
}

ll comp_userid;
bool sortByPriority(user_action a, user_action b)
{
    return num_mutual_frnds[a.user_id][comp_userid] < num_mutual_frnds[b.user_id][comp_userid] ? true : false;
}

void *readPost(void *args)
{
    while (1)
    {
        pthread_mutex_lock(&tcount);
        while (count_empty_feed == N)
            pthread_cond_wait(&tread, &tcount);
        pthread_mutex_unlock(&tcount);
        pthread_mutex_lock(&tlock);
        ll idx = -1;
        for (ll i = 0; i < N; i++)
        {
            if (!nodes[i].feed_queue.empty())
            {
                idx = i;
                break;
            }
        }

        vector<user_action> temp;
        while (!nodes[idx].feed_queue.empty())
        {
            temp.push_back(nodes[idx].feed_queue.front());
            nodes[idx].feed_queue.pop();
        }

        ll prior = nodes[idx].priority;
        pthread_mutex_unlock(&tlock);

        if (!prior)
        {
            comp_userid = idx;
            for (auto it : temp)
            {
                if (num_mutual_frnds[idx][it.user_id] == -1)
                {
                    calc_mutual_friends(idx, it.user_id);
                }
            }
            sort(temp.begin(), temp.end(), sortByPriority);
        }
        else
        {
            sort(temp.begin(), temp.end(), sortByTime);
        }
        pthread_mutex_lock(&tlock);

        for (auto it : temp)
            nodes[idx].feed_queue.push(it);
        string msg = "";
        while (!nodes[idx].feed_queue.empty())
        {
            user_action curr_action = nodes[idx].feed_queue.front();
            nodes[idx].feed_queue.pop();
            msg += "Read action number ";
            msg += to_string(curr_action.action_id);
            msg += " of type ";

            switch (curr_action.action_type)
            {
            case POST:
                msg += "POST";
                break;
            case COMMENT:
                msg += "COMMENT";
                break;
            case LIKE:
                msg += "LIKE";
                break;
            }

            msg += " posted by user ";
            msg += to_string(curr_action.user_id);
            msg += "created at time ";
            msg += ctime(&curr_action.created_time);
            msg += "\n";
        }
        pthread_mutex_unlock(&tlock);

        pthread_mutex_lock(&tcount);
        count_empty_feed++;
        pthread_mutex_unlock(&tcount);
        pthread_mutex_lock(&tlock);
        cout << msg << endl;
        ofstream log_File;
        string logFileName = "sns.log";
        log_File.open(logFileName, ios::app);
        if (log_File.is_open())
        {
            // write to the log file
            log_File << msg << endl;

            // close the log file
            log_File.close();
        }
        else
        {
            cout << "Failed to open log file." << endl;
        }
        pthread_mutex_unlock(&tlock);
        sleep(1);
    }

    return NULL;
}
void *pushUpdate(void *args)
{
    while (1)
    {
        pthread_mutex_lock(&tlock);
        while (shared_queue.empty())
        {
            pthread_cond_wait(&tcond, &tlock);
        }

        user_action new_action = shared_queue.front();
        shared_queue.pop();
        pthread_mutex_unlock(&tlock);

        for (ll i = 0; i < nodes[new_action.user_id].degree; i++)
        {
            ll v = adj_list[new_action.user_id][i];
            bool empty = false;
            pthread_mutex_lock(&tlock);
            if (nodes[v].feed_queue.empty())
            {
                empty = true;
            }
            nodes[v].feed_queue.push(new_action);
            pthread_mutex_unlock(&tlock);
            if (empty)
            {
                pthread_mutex_lock(&tcount);
                count_empty_feed--;
                pthread_mutex_unlock(&tcount);
            }
            pthread_cond_broadcast(&tread);
        }

        sleep(1);
    }
    return NULL;
}
int main()
{
    init_graph();
    // cout<<"ok1"<<"\n";

    pthread_t threads[36];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    if (pthread_mutex_init(&tlock, NULL) != 0)
    {
        printf("\n mutex init has failed\n");
        return 1;
    }
    if (pthread_mutex_init(&tcount, NULL) != 0)
    {
        printf("\n mutex init has failed\n");
        return 1;
    }
    for (ll i = 0; i < 36; i++)
    {
        if (i == 0)
        {
            pthread_create(&threads[i], &attr, userSimulator, NULL);
        }
        else if (i <= 10)
        {
            pthread_create(&threads[i], &attr, readPost, NULL);
        }
        else
        {
            pthread_create(&threads[i], &attr, pushUpdate, NULL);
        }
    }
    for (ll i = 0; i < 36; i++)
        pthread_join(threads[i], NULL);

    pthread_mutex_destroy(&tlock);
    pthread_mutex_destroy(&tcount);
    return (0);
}
