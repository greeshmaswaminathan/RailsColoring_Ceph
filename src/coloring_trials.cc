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

extern "C" {    // another way
 #include <ch-placement.h>
 #include <ch-placement-crush.h>  
 #include <ch-placement-oid-gen.h>
 #include <codes/jenkins-hash.h>
	//struct crush_map *crush_create();
	//struct ch_placement_instance* ch_placement_initialize_crush(struct crush_map *map, __u32 *weight, int n_weight);
};

struct options
{
    unsigned int num_servers;
    unsigned int virt_factor;
    unsigned int max_objs;
    unsigned int pgs;
    long unsigned int max_bytes;
    unsigned int replication;
    unsigned int random_seed;
    unsigned int fail_server;
    unsigned int print_fail;
    char* type;
    char* placement;
    char* filename;
    char* params;
};

struct server_data
{
    unsigned int server_index;
    /* objects that might reside on this server during the course of a simulation */
    unsigned int potential_obj_count; 
    struct obj* potential_obj_array;
};

struct server_idx_infile
{
    unsigned int server_index;
    off_t offset;
    unsigned int obj_count;
};
static int usage (char *exename)
{
    std::cout << "in usage \n";
    fprintf(stderr, "Usage: %s [options] <output-filename>\n", exename);
    fprintf(stderr, "    -s <number of servers>\n");
    fprintf(stderr, "    -v <virtual nodes per physical node>\n");
    fprintf(stderr, "    -o <max number of objects>\n");
    fprintf(stderr, "    -b <max number of bytes>\n");
    fprintf(stderr, "    -r <replication factor>\n");
    fprintf(stderr, "    -t <object generator: basic/random/hist_stripe/hist_hadoop>\n");
    fprintf(stderr, "    -p <placement algorithm>\n");
    fprintf(stderr, "    -x <parameters to data set generator, optional>\n");
    fprintf(stderr, "    -g <num placement groups>\n");
    fprintf(stderr, "    -q <random seed for object generation>\n");
    fprintf(stderr, "    -f <server>\n");

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

    while((one_opt = getopt(argc, argv, "s:o:b:r:t:hp:x:v:g:q:f:")) != EOF)
    {
        switch(one_opt)
        {
            case 's':
                ret = sscanf(optarg, "%u", &opts->num_servers);
                if(ret != 1)
                    return(NULL);
                break;
            case 'q':
                ret = sscanf(optarg, "%u", &opts->random_seed);
                if(ret != 1)
                    return(NULL);
                break;
            case 'g':
                ret = sscanf(optarg, "%u", &opts->pgs);
                if(ret != 1)
                    return(NULL);
                break;
            case 'v':
                ret = sscanf(optarg, "%u", &opts->virt_factor);
                if(ret != 1)
                    return(NULL);
                break;
            case 'o':
                ret = sscanf(optarg, "%u", &opts->max_objs);
                if(ret != 1)
                    return(NULL);
                break;
            case 'b':
                ret = sscanf(optarg, "%lu", &opts->max_bytes);
                if(ret != 1)
                    return(NULL);
                break;
            case 'r':
                ret = sscanf(optarg, "%u", &opts->replication);
                if(ret != 1)
                    return(NULL);
                break;
            case 't':
                opts->type = strdup(optarg);
                if(!opts->type)
                    return(NULL);
                break;
            case 'p':
                opts->placement = strdup(optarg);
                if(!opts->placement)
                    return(NULL);
                break;
            case 'x':
                opts->params = strdup(optarg);
                if(!opts->params)
                    return(NULL);
                break;
            case 'f':
                opts->fail_server = atoi(optarg);
                opts->print_fail = 1;
                break;
            case '?':
                usage(argv[0]);
                exit(1);
        }
    }

    if(optind != argc-1)
    {
        return(NULL);
    }
    opts->filename = strdup(argv[optind]);

    if(opts->replication < 2)
        return(NULL);
    if(opts->num_servers < (opts->replication+1))
        return(NULL);
    if(opts->virt_factor < 1)
        return(NULL);
    if(opts->max_objs < 1)
        return(NULL);
    if(!opts->placement)
        return(NULL);
    if(!opts->type)
        return(NULL);

    return(opts);
}



static int setup_crush(struct options *ig_opts,
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
    struct crush_bucket* svr_buckets[64]; /* which server? one bucket per rack */
    struct crush_bucket* rack_buckets[8]; /* which rack? one bucket per row */
    struct crush_bucket* row_bucket;  /* which row? */
    int nrows;
    int bucket_id = -2;
    int j;

    *n_weight = ig_opts->num_servers;

    *weight = malloc(sizeof(**weight)*ig_opts->num_servers);
    weights = malloc(sizeof(*weights)*ig_opts->num_servers);
    items = malloc(sizeof(*items) * ig_opts->num_servers);
    if(!(*weight) || !weights || !items || !map)
    {
        return(-1);
    }
    
    for(i=0; i< ig_opts->num_servers; i++)
    {
        items[i] = i;
        weights[i] = 0x10000;
        (*weight)[i] = 0x10000;
    }

    *map = crush_create();
    assert(*map);
    if(strcmp(ig_opts->placement, "crush") == 0)
    {
        bucket = crush_make_bucket(*map, CRUSH_BUCKET_STRAW, CRUSH_HASH_DEFAULT, 1,
                ig_opts->num_servers, items, weights);
        ret = crush_add_bucket(*map, -2, bucket, &id);
        assert(ret == 0);
    }
    else if(strcmp(ig_opts->placement, "crush-nested") == 0)
    {
        /* big hack; we just generate a few specific configurations */
        assert(ig_opts->num_servers == 1024 || 
            ig_opts->num_servers == 512 || 
            ig_opts->num_servers == 256);

        /* triple nested: 16 servers per rack, 8 racks per row, 2, 4, or 8
         * rows to get a total of 256, 512, or 1024 servers
         */
        int crush_bucket_type = CRUSH_BUCKET_STRAW2;
        row_bucket = crush_make_bucket(*map, crush_bucket_type , CRUSH_HASH_DEFAULT, 1, 0/*2*/, items, weights);
        assert(row_bucket);
        nrows = ig_opts->num_servers / 128;
        for(i=0; i<nrows; i++)
        {
            rack_buckets[i] = crush_make_bucket(*map, crush_bucket_type, CRUSH_HASH_DEFAULT, 2, 0 /*8*/, items, weights);
            assert(rack_buckets[i]);

            ret = crush_add_bucket(*map, bucket_id, rack_buckets[i], &id);
            assert(ret == 0);
            bucket_id--;

            ret = crush_bucket_add_item(*map, row_bucket, id, 0x10000);
            assert(ret == 0);
        }

        for(i=0; i<(ig_opts->num_servers/16); i++)
        {
            for(j=0; j<16; j++)
                items[j] = i*16+j;

            svr_buckets[i] = crush_make_bucket(*map, crush_bucket_type,
                CRUSH_HASH_DEFAULT, 3, 16, items, weights);
            assert(svr_buckets[i]);

            ret = crush_add_bucket(*map, bucket_id, svr_buckets[i], &id);
            assert(ret == 0);
            bucket_id--;

            ret = crush_bucket_add_item(*map, rack_buckets[i/8], id, 0x10000);
            assert(ret == 0);
        }

        ret = crush_add_bucket(*map, bucket_id, row_bucket, &id);
        assert(ret == 0);
        bucket_id--;
    }
#if 0
    straw_bucket = (struct crush_bucket_straw*) bucket;
    for (i=0; i < ig_opts->num_servers; i++)
    {
        printf("[%03d] weight: %d straw: 0x%x\n",
            i, straw_bucket->item_weights[i], straw_bucket->straws[i]);
    }
#endif
    
    crush_finalize(*map);

    rule = crush_make_rule(3, 0, 1, 1, 10);
    assert(rule);

    crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, id, 0);
    crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSELEAF_FIRSTN, 8, 0);
    crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);

    ret = crush_add_rule(*map, rule, 0);
    assert(ret == 0);

    return(0);
}

static int color(std::map<int, std::string> &server_color_map, std::map<int,std::list<int> > &pg_server_map,std:: set<int> &conflict_pgs, std::string colorName ){
   std::map<int,std::list<int> >::iterator pg_it;
   std::list<int>::iterator server_it;
  
   std::map<int, std::string>::iterator server_color_it;
   int coloredCount = 0;
   bool coloring_success = false;
   //int first_server_tocolor = -1;
   for (pg_it = pg_server_map.begin(); pg_it!=pg_server_map.end(); ++pg_it) {
      std::list<int> servers = pg_it ->second;
      coloring_success = false;
      for(server_it = servers.begin(); server_it!=servers.end(); ++server_it){
	server_color_it = server_color_map.find(*server_it);
	if(server_color_it != server_color_map.end()){
	   if(server_color_it->second == colorName){
		//std::cout << "Already colored " << server_color_it->second;
		coloring_success = true;
		break;
	   }
        }else{
	   server_color_map[*server_it] = colorName;
	   coloring_success = true;
	   coloredCount ++;
	   break;
	}
      }
      if(!coloring_success){
	//std::cout << "coloring failed for pg " << pg_it->first;
	conflict_pgs.insert(pg_it->first);
      }
   }
    //std::cout << it->first << " => " << it->second << '\n';
   return coloredCount;
}

static int color_2(std::map<int, std::string> &server_color_map, std::map<int,std::list<int> > &pg_server_map,std:: set<int> &conflict_pgs, std::list<std::string> colors ){
   std::map<int,std::list<int> >::iterator pg_it;
   std::list<int>::iterator server_it;
   int red_count = 0;
   int green_count = 0;
   int blue_count = 0;

   std::map<int, std::string>::iterator server_color_it;
   int coloredCount = 0;
   bool coloring_success = false;
   //int first_server_tocolor = -1;
   for (pg_it = pg_server_map.begin(); pg_it!=pg_server_map.end(); ++pg_it) {
      std::list<int> servers = pg_it ->second;
      coloring_success = false;
      std::string colorName;
      std::list<std::string>::iterator colors_it;
      for(colors_it = colors.begin(); colors_it!=colors.end(); ++colors_it){
	colorName = *colors_it;
 	for(server_it = servers.begin(); server_it!=servers.end(); ++server_it){
        server_color_it = server_color_map.find(*server_it);
        if(server_color_it != server_color_map.end()){
           if(server_color_it->second == colorName){
                //std::cout << "Already colored " << server_color_it->second;
                coloring_success = true;
                break;
           }
        }else{
           server_color_map[*server_it] = colorName;
           coloring_success = true;
	   if(colorName == "red") red_count++;
	   else if (colorName == "green") green_count++;
	   else blue_count++;
           coloredCount ++;
           break;
        }
      }
      if(!coloring_success){
        //std::cout << "coloring failed for pg " << pg_it->first;
        conflict_pgs.insert(pg_it->first);
      }     
      }

   }
    //std::cout << it->first << " => " << it->second << '\n';
   std::cout << "red :" << red_count << ",  green : "<< green_count << ", blue : "<< blue_count << "\n";
   return coloredCount;
}


int main(int argc, char **argv){
   struct options *ig_opts = NULL;
   int ret;
   int n_weight;
   struct crush_map *map;
    __u32 *weight;
   struct ch_placement_instance* instance;
   unsigned long total_byte_count = 0;
   unsigned long total_obj_count = 0;
   struct obj* total_objs = NULL;
   struct server_data* server_array;
   unsigned long i;
   unsigned int* server_array_cnt;
   struct server_idx_infile* server_idx_array;
   uint64_t *pg_histo;
   uint64_t input_oid;
   uint32_t h1, h2;
   int max_replication;
   int index;
   std::map<int, std::string> server_color_map;   
   std::map<int,std::list<int> > pg_server_map; 
   std::set<int> conflict_pgs;
   std::set<int>::iterator conflict_pgs_it;
   int red_count;
   int green_count;
   int blue_count; 

   ig_opts = parse_args(argc, argv);
   if(!ig_opts)
   {
   	usage(argv[0]);
        return(-1);
   }
   max_replication = ig_opts->replication;
   if(max_replication > ig_opts->num_servers)
      max_replication = ig_opts->num_servers;

   ret = setup_crush(ig_opts, &map, &weight, &n_weight);
   if(ret < 0)
   {
   	fprintf(stderr, "Error: failed to set up CRUSH.\n");
	std::cout << "Failed to setup crush. \n";
        return(-1);
   }
   std::cout << "ret value :" << ret << "\n";
   instance = ch_placement_initialize_crush(map, weight, n_weight);
   assert(instance);
   
    // printf("Generating up to %u objects...\n", ig_opts->max_objs);
    oid_gen(ig_opts->type, instance, ig_opts->max_objs, ig_opts->max_bytes,
        ig_opts->random_seed, ig_opts->replication, ig_opts->num_servers, ig_opts->params,
        &total_byte_count, &total_obj_count, &total_objs);

    //printf("Produced %lu objects with %lu total bytes (not counting replication).\n", total_obj_count, total_byte_count);
   
    /* set up array to track per-server object data */
    server_array = malloc(ig_opts->num_servers*sizeof(*server_array));
    assert(server_array);
    memset(server_array, 0, ig_opts->num_servers*sizeof(*server_array));
    server_array_cnt = malloc(ig_opts->num_servers*sizeof(*server_array_cnt));
    assert(server_array_cnt);
    memset(server_array_cnt, 0, ig_opts->num_servers*sizeof(*server_array_cnt));
    server_idx_array = malloc(ig_opts->num_servers*sizeof(*server_idx_array));
    assert(server_idx_array);
    memset(server_idx_array, 0, ig_opts->num_servers*sizeof(*server_idx_array));

    for(i=0; i<ig_opts->num_servers; i++)
    {
        server_array[i].server_index = i;
    }

    if(ig_opts->pgs)
    {
        pg_histo = malloc(ig_opts->pgs*sizeof(*pg_histo));
        assert(pg_histo);
        memset(pg_histo, 0, ig_opts->pgs*sizeof(*pg_histo));
    }

    //printf("Calculating server placements\n");
    for(i=0; i<ig_opts->pgs/*total_obj_count*/; i++)
    {
	/*if(ig_opts->pgs > 0)
        {
            h1 = 0;
            h2 = 0;
            bj_hashlittle2(&total_objs[i].oid, sizeof(total_objs[i].oid),
                &h1, &h2);
            input_oid = h1 + (((uint64_t)h2)<<32);
            input_oid = input_oid % ig_opts->pgs;
            pg_histo[input_oid]++;
        }
        else
        {
            input_oid = total_objs[i].oid;
        }*/
	input_oid = i;
        ch_placement_find_closest(instance, input_oid, max_replication, total_objs[i].server_idxs);
	//std::cout << "input " << input_oid << " " ;
	for(index=0; index<max_replication; index++){
	   pg_server_map[input_oid].push_back(total_objs[i].server_idxs[index]);	
	   //std::cout << total_objs[i].server_idxs[index] << " ";				
	}    
	std::cout << "\n";

    }
    
    //std::cout << "size of map" << pg_server_map.size();
    /*red_count = color(server_color_map,pg_server_map,conflict_pgs,"red");
    green_count = color(server_color_map,pg_server_map,conflict_pgs,"green");
    blue_count = color(server_color_map,pg_server_map,conflict_pgs,"blue");
    std::cout << "******* results ******* \n";
    std::cout << "red servers:"<< red_count << " green servers: " << green_count << " blue servers: " << blue_count << "\n";
    std::cout << "coloring map size : " << server_color_map.size() << "\n" ;
    std::cout << "Placement groups with conflict: " << conflict_pgs.size() << "\n"; 
    std::cout << "********* Coloring pattern of each conflicted placement group ********** \n";
    for(conflict_pgs_it = conflict_pgs.begin(); conflict_pgs_it!=conflict_pgs.end(); ++conflict_pgs_it){
	int conflict_pg = *conflict_pgs_it;	
        std::list<int> servers_pg = pg_server_map.find(conflict_pg)->second;
        std::list<int>::iterator servers_pg_it;
	std::cout << "For pg: " << conflict_pg << " ";
	for(servers_pg_it  = servers_pg.begin(); servers_pg_it!=servers_pg.end(); ++servers_pg_it){
	    std::cout << server_color_map.find(*servers_pg_it)->second << " "; 
	}
	std::cout << "\n";
    }*/
   
   // approach 2
   std::cout << "approach 2 \n";
   std::list<std::string> colors;
   colors.push_back("red");
   colors.push_back("green");
   colors.push_back("blue");
   int total_colored_servers  = color_2(server_color_map,pg_server_map,conflict_pgs,colors);
   std::cout << "total colored servers: " << total_colored_servers << "\n";
   for(conflict_pgs_it = conflict_pgs.begin(); conflict_pgs_it!=conflict_pgs.end(); ++conflict_pgs_it){
        int conflict_pg = *conflict_pgs_it;
        std::list<int> servers_pg = pg_server_map.find(conflict_pg)->second;
        std::list<int>::iterator servers_pg_it;
        std::cout << "For pg: " << conflict_pg << " ";
        for(servers_pg_it  = servers_pg.begin(); servers_pg_it!=servers_pg.end(); ++servers_pg_it){
            std::cout << server_color_map.find(*servers_pg_it)->second << " ";
        }
        std::cout << "\n";
    }

   
}


