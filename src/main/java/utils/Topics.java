package utils;

public enum Topics {

    LOG_DATA_RAW {
        @Override
        public String toString() {
            return "LogDataRaw";
        }
    },
    CLICK_LOG {
        @Override
        public String toString() { return "ClickLog"; }
    },
    ORDER_COMPLETE_LOG {
        @Override
        public String toString() { return "OrderCompleteLog"; }
    },
    SEARCH_LOG {
        @Override
        public String toString() { return "SearchLog"; }
    },
    CART_LOG {
        @Override
        public String toString() { return "CartLog"; }
    },
    CLICK_RANKING {
        @Override
        public String toString() { return "ClickRanking"; }
    },
    CLICK_MATRIX {
        @Override
        public String toString() { return "ClickMatrix"; }
    };

    public String topicName(){
        return this.toString();
    }
}
