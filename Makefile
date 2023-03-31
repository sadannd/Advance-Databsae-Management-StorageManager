.PHONY: all
all: test_assign3 test_expr

test_assign3: test_assign3_1.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c record_mgr.c expr.c rm_serializer.c dberror.c 
	gcc -o test_assign3 test_assign3_1.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c record_mgr.c rm_serializer.c expr.c dberror.c

test_expr: test_expr.c dberror.c expr.c record_mgr.c rm_serializer.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c 
	gcc -o test_expr test_expr.c dberror.c expr.c record_mgr.c rm_serializer.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c 

.PHONY: clean
clean: clean_test_assign3 clean_test

clean_test_assign3:
	rm test_assign3 

clean_test:
	rm test_expr