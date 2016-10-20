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
    struct crush_bucket* color_buckets[8]; /* which rack? one bucket per row */
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
    if(strcmp(ig_opts->placement, "crush-nested") == 0)
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
            color_buckets[i] = crush_make_bucket(*map, crush_bucket_type, CRUSH_HASH_DEFAULT, 2, 0 /*8*/, items, weights);
            assert(color_buckets[i]);

            ret = crush_add_bucket(*map, bucket_id, color_buckets[i], &id);
            assert(ret == 0);
            bucket_id--;
	    std::cout << " Id after adding color_bucket " << id << "\n";

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
            std::cout << " Id after adding svr_bucket " << id << "\n";
            assert(ret == 0);
            bucket_id--;

            ret = crush_bucket_add_item(*map, color_buckets[i/8], id, 0x10000);
            assert(ret == 0);
        }

        ret = crush_add_bucket(*map, bucket_id, row_bucket, &id);
	std::cout << " Id after adding row_bucket " << id << "\n";
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

    rule = crush_make_rule(4, 0, 1, 1, 10);
    assert(rule);

    crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, id, 0);
    crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, 4, 2);
    crush_rule_set_step(rule, 2, CRUSH_RULE_CHOOSELEAF_FIRSTN, 1, 0);
    crush_rule_set_step(rule, 3, CRUSH_RULE_EMIT, 0, 0);

    ret = crush_add_rule(*map, rule, 0);
    assert(ret == 0);

    return(0);
}

std::string get_color(int server_id){
   int color = (server_id)/128;
   switch (color){
	case 0 : return "red";
	case 1 : return "green";
	case 2 : return "blue";
	case 3 : return "orange";
   }
}



int main(int argc, char **argv){
   
   unsigned long i;
   uint64_t input_oid;
   int index;
   std::map<int,std::list<int> > pg_server_map; 

   struct options *ig_opts = NULL;
   ig_opts = parse_args(argc, argv);
   if(!ig_opts)
   {
   	usage(argv[0]);
        return(-1);
   }
   
   int max_replication = ig_opts->replication;
   if(max_replication > ig_opts->num_servers)
      max_replication = ig_opts->num_servers;

   int n_weight;
   struct crush_map *map;
   __u32 *weight;
   int ret = setup_crush(ig_opts, &map, &weight, &n_weight);
   if(ret < 0)
   {
	std::cout << "Failed to setup crush. \n";
        return(-1);
   }
   //std::cout << "ret value :" << ret << "\n";
   struct ch_placement_instance* instance = ch_placement_initialize_crush(map, weight, n_weight);
   assert(instance);
   
   
    //printf("Calculating server placements\n");
    for(i=0; i<ig_opts->pgs/*total_obj_count*/; i++)
    {

	input_oid = i;
        long unsigned int mapped_servers[max_replication];
        ch_placement_find_closest(instance, input_oid, max_replication, mapped_servers);
	//std::cout << "input " << input_oid << " " ;
        std::set<std::string> pg_colors;
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
    
    
}



