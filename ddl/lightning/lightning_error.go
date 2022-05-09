package lightning


const (
	// Error message
	LERR_ALLOC_MEM_FAILED      string = "Lightning: Alloc memory failed."
	LERR_OUT_OF_MAX_MEM        string = "Lightning: Memory is used up for Lightning add index."
	LERR_NO_MEM_TYPE           string = "Lightning: Unking structure for Lightning add index."
	LERR_CREATE_BACKEND_FAILED string = "Lightning: Build lightning backend failed, will use kernel index reorg method to backfill the index."
	LERR_CREATE_ENGINE_FAILED  string = "Lightning: Build lightning engine failed, will use kernel index reorg method to backfill the index."
	LERR_CREATE_CONTEX_FAILED  string = "Lightning: Build lightning worker context failed, will use kernel index reorg method to backfill the index."
	LERR_GET_ENGINE_FAILED     string = "Lightning: Use key get engininfo failed."
	LERR_GET_STORAGE_QUOTA     string = "Lightning: Get storage quota err:"
	
	// Warning message    
	LWAR_ENV_INIT_FAILD        string = "Lightning: initialize environment failed"

	// Info message
	LInfo_ENV_INIT_SUCC        string = "Lightning: Init global lightning backend environment finished."
)