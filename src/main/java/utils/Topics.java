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
    },
    ORDER_COMPLETE_LOG {
        @Override
        public String toString() { return "order_complete_log"; }
    },
    SEARCH_LOG {
        @Override
        public String toString() { return "search_log"; }
    },
    CART_LOG {
        @Override
        public String toString() { return "cart_log"; }
    },
    CLICK_RANKING {
        @Override
        public String toString() { return "click_ranking"; }
    },
    CLICK_MATRIX {
        @Override
        public String toString() { return "click_matrix"; }
    };

    public String topicName(){
        return this.toString();
    }
}
