#include<iostream>
#include<bits/stdc++.h>
#include<pthread.h>
#include<ctime>
#include<cmath>
using namespace std;
typedef long long ll;
enum Action {POST, COMMENT, LIKE};
typedef struct _user_action{
    ll user_id;
    ll action_id;
    Action action_type;
    time_t created_time;

} user_action;
typedef struct _node{
    ll id;
    ll priority;
    ll degree;
    ll action_count_post;
    ll action_count_like;
    ll action_count_comment;
    queue<user_action> wall_queue,feed_queue;

} node;
const ll N = 37700;
vector<ll>adj_list[N];
vector<node>nodes(N);

void init_graph(){
    ifstream input_file("musae_git_edges.csv");
    string line;
    getline(input_file,line);
    while(getline(input_file,line)){
        stringstream linestream(line);
        vector<ll>nodenum;
        string nodestr;
        while(getline(linestream,nodestr,',')){
            nodenum.push_back((ll)atoi(nodestr.c_str()));
        }
        if(nodenum.size() != 2){
            perror("Edge specification incorrect");
            exit(0);
        }
        adj_list[nodenum[0]].push_back(nodenum[1]);
        adj_list[nodenum[1]].push_back(nodenum[0]);
    }
    for(ll i = 0; i < N; i++){
        nodes[i].id = i;
        nodes[i].priority = rand()%2;
        nodes[i].degree = adj_list[i].size();
        nodes[i].action_count_comment = nodes[i].action_count_like = nodes[i].action_count_post = 0LL;
    }
}
void *userSimulator(void *args){
    while(1){
        srand(time(0));
        for(ll i = 0; i < 100; i++){
            ll node_idx = rand()%N;
            ll no_actions = (ll)floor(log2(nodes[node_idx].degree));
            for(ll j = 0; j < no_actions; j++){
                ll act_type = rand()%3;
                user_action new_act;
                new_act.user_id = node_idx;
                new_act.created_time = time(NULL);
                switch(act_type){
                    case 0:
                        new_act.action_type = POST;
                        nodes[node_idx].action_count_post++;
                        new_act.action_id = nodes[node_idx].action_count_post;
                        break;
                    case 1:
                        new_act.action_type = COMMENT;
                        nodes[node_idx].action_count_comment++;
                        new_act.action_id = nodes[node_idx].action_count_comment;
                        break;
                    case 2:
                        new_act.action_type = LIKE;
                        nodes[node_idx].action_count_like++;
                        new_act.action_id = nodes[node_idx].action_count_like;
                        break;
                }
                nodes[node_idx].wall_queue.push(new_act);
            }
        }
        sleep(120);
    }
}
void *readPost(void *args){

}
void *updatePost(void *args){

}
int main(){
    init_graph();
    pthread_t threads[36];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    for(ll i = 0; i < 36; i++){
        if(i == 0){
            pthread_create(&threads[i],&attr,userSimulator,NULL);
        }
        else if(i <= 10){
            pthread_create(&threads[i],&attr,readPost,NULL);
        }
        else{
            pthread_create(&threads[i],&attr,updatePost,NULL);
        }
    }
    for(ll i = 0; i < 36; i++)
        pthread_join(threads[i],NULL);
    return(0);
}