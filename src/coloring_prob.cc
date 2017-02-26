#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <hash.h>
#include <assert.h>
#include <map>
#include <list>
#include <set>
#include <vector>
using namespace std;

extern "C" {    // another way
 #include <ch-placement.h>
 #include <ch-placement-crush.h>  
};

struct options
{
    unsigned int num_servers;
    unsigned int pgs;
    unsigned int replication;
    char* placement;
};

static int usage (char *exename)
{
    std::cout << "in usage \n";
    fprintf(stderr, "Usage: %s [options] \n", exename);
    fprintf(stderr, "    -s <number of servers>\n");
    fprintf(stderr, "    -r <replication factor>\n");
    fprintf(stderr, "    -p <placement algorithm>\n");
    fprintf(stderr, "    -g <num placement groups>\n");

    exit(1);
}

static struct options *parse_args(int argc, char *argv[])
{
    struct options *opts = NULL;
    int ret = -1;
    int one_opt = 0;

    opts = (struct options*)malloc(sizeof(*opts));
    if(!opts)
        return(NULL);
    memset(opts, 0, sizeof(*opts));

    while((one_opt = getopt(argc, argv, "s:r:p:g:")) != EOF)
    {
        switch(one_opt)
        {
            case 's':
                ret = sscanf(optarg, "%u", &opts->num_servers);
                if(ret != 1)
                    return(NULL);
                break;
            case 'g':
                ret = sscanf(optarg, "%u", &opts->pgs);
                if(ret != 1)
                    return(NULL);
                break;
            case 'r':
                ret = sscanf(optarg, "%u", &opts->replication);
                if(ret != 1)
                    return(NULL);
                break;
            case 'p':
                opts->placement = strdup(optarg);
                if(!opts->placement)
                    return(NULL);
                break;
            case '?':
		printf("one opt %c ",one_opt);
                usage(argv[0]);
                exit(1);
        }
    }

    if(opts->replication < 2)
        return(NULL);
    if(opts->num_servers < (opts->replication))
        return(NULL);
    if(!opts->placement)
        return(NULL);

    return(opts);
}

static int get_rows(struct options *ig_opts){
    return 1;
}

int add_server(int disk_count, int start_color, int start_item, int &bucket_id, int &id, int *weights, struct crush_map **map){
	    int items[disk_count];
	    int colors[disk_count];
	    for(int j=0; j<disk_count; j++){
                int item_id = start_item + j;
                items[j] = item_id;
                colors[j] = (start_color + j) % 4;
            }
            struct crush_bucket* svr_bucket = crush_make_bucket(*map, CRUSH_BUCKET_STRAW2,
                CRUSH_HASH_DEFAULT, 3, disk_count, items, weights);
            assert(svr_bucket);
            int ret = crush_add_bucket(*map, bucket_id, svr_bucket, &id);
            crush_bucket_map_colors(svr_bucket, items, colors);
            std::cout << " Id after adding svr_bucket " << id << "\n";
 	    assert(ret == 0);
	    return ret;
}

static int setup_crush_3(struct options *ig_opts,
    struct crush_map **map, __u32 **weight, int *n_weight)
{
    struct crush_bucket* bucket;
    struct crush_bucket_straw *straw_bucket;
    int i;
    int *items;
    int *weights;
    int ret;
    int id;
    struct crush_rule* rule;
    struct crush_bucket* svr_buckets[2]; /* which server? one bucket per rack */
    struct crush_bucket* rack_bucket; /* which rack? one bucket per row */
    struct crush_bucket* row_bucket;  /* which row? */
    int nrows;
    int bucket_id = -2;
    int j;

    *n_weight = ig_opts->num_servers;

    *weight = (__u32 *)malloc(sizeof(**weight)*ig_opts->num_servers);
    weights = (int *)malloc(sizeof(*weights)*ig_opts->num_servers);
    items = (int *)malloc(sizeof(*items) * ig_opts->num_servers);
    if(!(*weight) || !weights || !items || !map)
    {
        return(-1);
    }

    for(i=0; i< ig_opts->num_servers; i++)
    {
        items[i] = i;
        //if(i == 342 || i == 102 || i == 110 || i == 106 || i == 238 || i == 234 || i == 226 || i == 230){
                //weights[i] = 0;
                //(*weight)[i] = 0;
        //}
        //else{
                weights[i] = 0x10000;
                (*weight)[i] = 0x10000;
        //}
    }
    *map = crush_create();
    assert(*map);
    if(strcmp(ig_opts->placement, "crush-nested") == 0)
    {
        /* big hack; we just generate a few specific configurations */
        //assert(ig_opts->num_servers == 1024 ||
           // ig_opts->num_servers == 512 ||
            //ig_opts->num_servers == 256);

        /* triple nested: 16 servers per rack, 8 racks per row, 2, 4, or 8
         * rows to get a total of 256, 512, or 1024 servers
         */
        int crush_bucket_type = CRUSH_BUCKET_STRAW2;
        row_bucket = crush_make_bucket(*map, crush_bucket_type , CRUSH_HASH_DEFAULT, 1, 0/*2*/, items, weights);
        assert(row_bucket);
        nrows = get_rows(ig_opts);
        for(i=0; i<nrows; i++)
        {
            rack_bucket  = crush_make_bucket(*map, crush_bucket_type, CRUSH_HASH_DEFAULT, 2, 0 /*8*/, items, weights);
            assert(rack_bucket);

            ret = crush_add_bucket(*map, bucket_id, rack_bucket, &id);
            assert(ret == 0);
            bucket_id--;
            //std::cout << " Id after adding color_bucket " << id << "\n";

            ret = crush_bucket_add_item(*map, row_bucket, id, 0x10000);
            assert(ret == 0);
        }

	//add servers
	ret = add_server(4, 0, 0, bucket_id, id, weights, map);
	bucket_id--;
        ret = crush_bucket_add_item(*map, rack_bucket, id, 0x10000);
        assert(ret == 0);
        ret = add_server(4, 0, 4, bucket_id, id, weights, map);
	bucket_id--;
        ret = crush_bucket_add_item(*map, rack_bucket, id, 0x10000);
        assert(ret == 0);
        
	ret = crush_add_bucket(*map, bucket_id, row_bucket, &id);
        std::cout << " Id after adding row_bucket " << id << "\n";
        assert(ret == 0);
        bucket_id--;
    }

    crush_finalize(*map);

    rule = crush_make_rule(3, 0, 1, 1, 10);
    assert(rule);

    crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, id, 0);
    crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSELEAF_FIRSTN, 4, 0);
    //crush_rule_set_step(rule, 2, CRUSH_RULE_CHOOSE_FIRSTN, 2, 3);
    //crush_rule_set_step(rule, 3, CRUSH_RULE_CHOOSELEAF_FIRSTN, 2, 0);
    crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);

    /*rule = crush_make_rule(4, 0, 1, 1, 10);
    assert(rule);

    crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, id, 0);
    crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, 2, 3);
    crush_rule_set_step(rule, 2, CRUSH_RULE_CHOOSELEAF_FIRSTN, 2, 0);
    crush_rule_set_step(rule, 3, CRUSH_RULE_EMIT, 0, 0);*/

    ret = crush_add_rule(*map, rule, 0);
    assert(ret == 0);

    return(0);
}





std::string get_color(int server_id){
   // int color = (server_id)/128;
   int color = (server_id) % 4;
   switch (color){
	case 0 : return "red";
	case 1 : return "green";
	case 2 : return "blue";
	case 3 : return "orange";
   }
}

void test_placement(int n_weight,struct crush_map *map,__u32 *weight,struct options *ig_opts){
   unsigned long i;
   uint64_t input_oid;
   int index;
   std::map<int,std::list<int> > pg_server_map; 
   int max_replication = ig_opts->replication;
   if(max_replication > ig_opts->num_servers)
      max_replication = ig_opts->num_servers;



   //std::cout << "ret value :" << ret << "\n";
   struct ch_placement_instance* instance = ch_placement_initialize_crush(map, weight, n_weight);
   assert(instance);
   
   
    //printf("Calculating server placements\n");
    for(i=0; i<ig_opts-> pgs/*total_obj_count*/; i++)
    {

	input_oid = i;
        long unsigned int mapped_servers[max_replication];
        ch_placement_find_closest(instance, input_oid, max_replication, mapped_servers);
	//std::cout << "input " << input_oid << " " ;
        std::set<std::string> pg_colors;
	std::cout << " placement group " << i << " \n";
	for(index=0; index<max_replication; index++){
	   pg_server_map[input_oid].push_back(mapped_servers[index]);	
	   std::cout << mapped_servers[index] <<":" <<get_color(mapped_servers[index])<<"  ";				
	   pg_colors.insert(get_color(mapped_servers[index]));
	}    
	if(pg_colors.size() != 4) {
	   std::cout << "Conflict!!!!";
	   break;
	}
	std::cout << "\n";

    }
    
   //std::cout << "Last placement group mapped " << i << " \n";
	
}

int count(__u32 step, const struct crush_rule_step *curstep, struct crush_map *map, list<crush_bucket*> buckets, const struct crush_rule *rule){
	/*int replica = curstep->arg1;
	int item_size = bucket->size;
	int copies = (replica <= item_size) ? replica : item_size;
	int i = 0;
	int success_count = 0;
	const struct crush_rule_step *nextstep = &rule->steps[step++];	
	//iterate over the passed buckets and add them to the same list.
	list<list<crush_bucket*> > permuted_buckets =  get_permutations(bucket, copies);
	for(i = 0; i < permuted_buckets.size() ; i++){
		
		success_count  += count(step++, nextstep, map,permuted_buckets[i] , rule);
	}
	return success_count;
	*/	
}
void  permute(vector<__s32> items,vector<__s32> current, int cur_index, int permute_length, vector<vector<__s32> >& results){
	if(permute_length <= 0) return;
	vector<__s32> item_copy(items);
	swap(item_copy[0], item_copy[cur_index]);
	int i = 0;
	cout << "current size " << current.size()<< "\n";
	for(i = current.size(); i < item_copy.size(); i++){
		vector<__s32> next(current);
		next.push_back(item_copy[i]);
		if(next.size() == permute_length) {
			results.push_back(next);
			cout << "added to results \n " ;
		}else{
			permute(items, next, i, permute_length, results);
		}
	} 
	
}
vector<vector<__s32> > get_permutations(crush_bucket* bucket, int copies){
	vector<vector<__s32> > results;
	cout << "bucket type" << bucket->type << "bucket size "<< bucket->size << '\n';
	if(bucket->type == 0) return results;
	__u32 size = bucket->size;
	vector<__s32> b_items(bucket->items, bucket->items + size);
	vector<__s32> current;
	cout << "b_items size" << b_items.size() << '\n' ;
	permute(b_items, current, 0, copies, results);
	return results;
}

void print_permutations(vector<vector<__s32> > results){
	std::cout << " \n Permutations "<< ":results size " << results.size() << '\n';
	for (vector<vector<__s32> >::iterator it = results.begin() ; it != results.end(); ++it)
	{
		vector<__s32> inner = *it;
		for (vector<__s32>::iterator it1 = inner.begin() ; it1 != inner.end(); ++it1){
			std::cout << ' ' << *it1;
    		}
  		std::cout << '\n';
	}

}

void  getall_buckets(struct crush_map *map, __u16 type,struct crush_bucket *bucket, vector<__s32>& result){
	int i = 0;
	for(i = 0; i < bucket->size; i++){
		__s32 item = bucket->items[i];
		if(item < 0){
			crush_bucket *b = map->buckets[-1-item];
			if(b->type == type) result.push_back(item);
			else 
			{
				getall_buckets(map, type, b, result);
			}
		}else{
			if(type == 0)
				result.push_back(item);
			else
				break;
		}
	}
}

void test_getall_buckets(struct crush_map *map, struct crush_bucket *bucket){
	vector<__s32> res;
	getall_buckets(map,3, bucket, res);
	int i =0;
	cout << "All buckets *********";
	for(int i = 0; i < res.size(); i++)
		cout << res[i] << " ";
	cout << "\n";
}

void combine(vector<vector<__s32> > v1, vector<vector<__s32> > v2, vector<vector<__s32> >& output){
	int i = 0;
	int j = 0;
	for(i = 0; i< v1.size(); i++){
		vector<__s32> combined(v1[i]);
		for(j = 0; j < v2.size(); j++){
			combined.insert(combined.end(), v2[j].begin(), v2[j].end());
		}
		output.push_back(combined);
	}
}


vector<vector<__s32> > form_output(vector<vector<vector<__s32> > > input, int cur_index, vector<vector<__s32> > output){
	cout << "input size " << input.size() << ", current index " << cur_index << "\n";
	if(cur_index == input.size()){
		cout << "returning from form_output"; 
		return output;
	}
	vector<vector<__s32> > merged_output;
	int i;
	vector<vector<__s32> > cur = input[cur_index];
	for(i = 0; i < cur.size(); i++){
		int j;
		for(j = 0; j < output.size(); j++){
			vector<__s32> v = output[j];
			v.insert(v.end(), cur[i].begin(), cur[i].end());
			merged_output.push_back(v);
		}
	} 
	return form_output(input, ++cur_index, merged_output);
}

void combine(vector<vector<vector<__s32> > > input, vector<vector<__s32> >& output){
	int i = 0;
	for(int i = 0; i < input.size(); i++) {
		swap(input[0], input[i]);
		vector<vector<__s32> > temp_output = form_output(input, 1, input[0]);
		output.insert(output.end(),temp_output.begin(), temp_output.end());
	}	
}



vector<vector<__s32> > fill_next_items(struct crush_map *map, vector<vector<__s32> > cur_output,  __u16 type, int replica_count){
	vector<vector<__s32> > new_output;
	int i = 0;
	for(int i = 0; i < cur_output.size(); i++) {
		vector<__s32> inner = cur_output[i];
		vector<vector<vector<__s32> > > indiv_results;
		int j = 0;
		for(int j = 0; j < inner.size(); j++){
			struct crush_bucket* bucket = map->buckets[-1-inner[j]];
			vector<vector<__s32> > results;
			int child_type = 0;
			if(bucket->items[0] < 0){
				child_type = map->buckets[-1-bucket->items[0]]->type;
			}
			if(type == child_type) {
				results = get_permutations(bucket, replica_count);
				
			}else{
				vector<__s32> res;				
				getall_buckets(map,type, bucket, res);		
				vector<__s32> current;	
        			permute(res, current, 0, replica_count, results);				
			}
			
			indiv_results.push_back(results);
		}
		combine(indiv_results, new_output);	
	}
	return new_output;
}

void test_fill_next_items(struct crush_map *map){
	vector<vector<__s32> > cur_output;
	vector<__s32> inner;
	//inner.push_back(-3);
	inner.push_back(-5);
	cur_output.push_back(inner);
	vector<vector<__s32> > new_output = fill_next_items(map,cur_output, 0, 2);
	cout << "\n test fill items \n " ;
	for (vector<vector<__s32> >::iterator it = new_output.begin(); it != new_output.end(); ++it){
                vector<__s32> vec = *it;
                for (vector<__s32>::iterator it1 = vec.begin(); it1 != vec.end(); ++it1) {
                        cout << ' ' << *it1;
                }
                cout << '\n';
        }
	
}


void test_combine(){
	__s32 a1[] = {1,2};
	vector<__s32> v1(a1, a1 + 2);
	__s32 a2[] = {3,4};
        vector<__s32> v2(a2, a2 + 2);
	vector<vector<__s32> > v11;
	v11.push_back(v1);
	v11.push_back(v2);
	__s32 a3[] = {5,6};
        vector<__s32> v3(a3, a3 + 2);
        __s32 a4[] = {7,8};
        vector<__s32> v4(a4, a4 + 2);
        vector<vector<__s32> > v12;
        v12.push_back(v3);
        v12.push_back(v4);
	
	vector<vector<vector<__s32> > > input;
	input.push_back(v11);
	input.push_back(v12);


	vector<vector<__s32> > output;
	__s32 a5[] = {100,200};
	vector<__s32> v5(a5, a5 + 2);	
	output.push_back(v5);

	combine(input, output);

	cout << "combine ********* \n";
	for (vector<vector<__s32> >::iterator it = output.begin(); it != output.end(); ++it){
		vector<__s32> vec = *it; 
		for (vector<__s32>::iterator it1 = vec.begin(); it1 != vec.end(); ++it1) {
			cout << ' ' << *it1;
		}
  		cout << '\n';
	}
}


void test_success_probability(struct crush_map *map){
	const struct crush_rule *rule = map->rules[0];
	__u32 step;
	vector<vector<__s32> > output;
	vector<__s32> op;
	
	for (step = 0; step < rule->len; step++) {
		const struct crush_rule_step *curstep = &rule->steps[step];
		struct crush_bucket* bucket;
		int replica = 0;
		int type = 0;
		switch (curstep->op) {
                	case CRUSH_RULE_TAKE:
				bucket = map->buckets[-1-curstep->arg1];
				op.push_back(curstep->arg1);
				output.push_back(op);
				break;
			case CRUSH_RULE_EMIT:
				break;
			case CRUSH_RULE_CHOOSE_FIRSTN:
				replica = curstep->arg1;
				type = curstep->arg2;
				output = fill_next_items(map, output, type, replica);
				break;
			case CRUSH_RULE_CHOOSELEAF_FIRSTN:
				replica = curstep->arg1;
                                type = curstep->arg2;
				output = fill_next_items(map, output, type, replica);
				break;
                	default:
                        printf(" unknown op %d at step %d\n",
                                curstep->op, step);
                        break;
                }		
	}	
	cout << "success probability ********* \n";
        for (vector<vector<__s32> >::iterator it = output.begin(); it != output.end(); ++it){
                vector<__s32> vec = *it;
                for (vector<__s32>::iterator it1 = vec.begin(); it1 != vec.end(); ++it1) {
                        cout << ' ' << *it1;
                }
                cout << '\n';
        }
}




int main(int argc, char **argv){
   
   struct options *ig_opts = NULL;
   ig_opts = parse_args(argc, argv);
   if(!ig_opts)
   {
   	usage(argv[0]);
        return(-1);
   }
   
   int n_weight;
   struct crush_map *map;
   __u32 *weight;
   int ret = setup_crush_3(ig_opts, &map, &weight, &n_weight);
   if(ret < 0)
   {
	std::cout << "Failed to setup crush. \n";
        return(-1);
   }

   //test_placement(n_weight, map, weight, ig_opts);
   test_success_probability(map);
   //test_fill_next_items(map);
   //test_combine();
}



