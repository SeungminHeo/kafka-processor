package utils;

public enum Topics {

    LOG_DATA_RAW {
        @Override
        public String toString() {
            return "log_data_raw";
        }
    },
    CLICK_LOG {
        @Override
        public String toString() { return "click_log"; }
    };


    public String topicName(){
        return this.toString();
    }
}
