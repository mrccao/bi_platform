from enum import Enum

TRANSACTION_TYPES = {
    # Gold Free Currency
    20132001: 'Admin mannually',
    999998301: 'Email validation',
    925011306: 'Lucky spin',
    925011307: 'Lucky spin(free spin)',
    925011410: 'Gold from complete daily mission',
    925011411: 'Gold from complete mission',
    30007777: 'ClubWPT migration',
    925011311: '金币过低再次发放',
    999998302: '链接和兑换码领取金币',
    925011312: '充值促销赠送的金币',

    # Gold
    925011308: 'Lucky spin(use ticket)',
    925011309: 'Lucky spin($0.99 purchase)',
    925011310: 'Lucky spin(use booster)',
    15111101: 'Store: buy avatar with gold',
    15111201: 'Store: buy gold with USD(TimeSlot)',
    15115300: 'Store: buy gold with USD(Poker)',
    20032001: '',
    20033101: 'Buy silver with gold',
    30000808: 'Store: buy gold with masterpoint',
    923092101: '',
    925011101: 'Gold consumed when enter and exit a table',
    925011201: 'Gold consumed when enter a tournament',
    925011202: 'Gold returned when exit a tournament',
    925011203: 'Award from tournament',
    925011204: 'Rebuy',
    925011206: 'Final addon',
    925011207: 'Bounty gold reward',
    925011402: 'Super product gold',
    925011601: 'Broadcast',
    925011602: 'Complimentary',
    925011605: 'Ring table insurance',
    925011612: 'Ring table reset',
    925011621: 'Store: buy Emoji, Charms, Ticket, Gifts',
    925011622: 'Table gift',
    925011903: 'Mobile login rewards',
    925011911: 'Mobile new users lessons',
    925011912: 'Mobile Texas Holden lessons',

    925011301: 'Regular free gold',
    925011302: 'New users free gold',

    # Silver Free Currency
    923118301: 'Game time increase',
    923118302: 'Game time increase',
    923118303: 'Game time increase',
    923118304: 'Game time increase',
    923118311: 'Collect award',
    923118312: 'Daily bonus',
    923118313: 'Newbie award',
    923118314: 'Upgrade award',

    # Silver
    923118401: '通过消息发送奖励',
    923118101: '连线环节游戏关卡1',
    923118102: '连线环节游戏关卡2',
    923118103: '连线环节游戏关卡3',
    923118104: '连线环节游戏关卡4',
    923118109: '连线环节新手奖励',
    923118111: '比倍环节游戏关卡1',
    923118112: '比倍环节游戏关卡2',
    923118113: '比倍环节游戏关卡3',
    923118114: '比倍环节游戏关卡4',
    923118131: '比倍环节获得Jackpot奖励',
    923118134: '连线环节获得Jackpot奖励',
    923118135: '连线环节获得新手Jackpot奖励',
    923118132: 'Free Spin结算',
    923118133: 'Bonus结算',
    923118136: '比倍环节Bonus结算',
}

GOLD_FREE_TRANSACTION_TYPES = [20132001, 999998301, 925011306, 925011307, 925011410, 925011411, 30007777, 925011311,
                               999998302]

SILVER_FREE_TRANSACTION_TYPES = [923118301, 923118302, 923118303, 923118304, 923118311, 923118312, 923118313, 923118314]

FREE_TRANSACTION_TYPES = GOLD_FREE_TRANSACTION_TYPES + SILVER_FREE_TRANSACTION_TYPES

FREE_TRANSACTION_TYPES_TUPLE = tuple(GOLD_FREE_TRANSACTION_TYPES + SILVER_FREE_TRANSACTION_TYPES)

PRODUCT_AND_PRODUCT_ORIG_MAPPING = {
    'charms': [28, 42, 39, 36, 33, 34, 44, 27, 41, 29, 43, 40, 16, 31, 32, 30, 35, 37, 38],
    'avatar': [53, 23, 22, 18, 10, 50, 14, 21, 20, 15, 48, 56, 49, 25, 52, 13, 51, 19, 26, 57, 12, 47, 17, 24, 55, 11,
               54],
    'emoji': [46], 'gold': [1], 'silver': [2], 'spin_accelerator': [8], 'spin_ticket': [9],
    'lucky_spin': [-1]}


class BaseEnum(Enum):
    def __str__(self):
        return self.value


class ADMIN_USER_ACTIVITY_ACTIONS(BaseEnum):
    LOGIN_FAILED = 'login_failed'
    LOGIN = 'login'
    LOGOUT = 'logout'
    PAGE_VIEW = 'page_view'
    CSV_EXPORT = 'csv_export'


class ADMIN_USER_ROLES(BaseEnum):
    ROOT = 'root'
    ADMIN = 'admin'
    MANAGER = 'manager'


class ADMIN_USER_QUERY_STATUSES(BaseEnum):
    CANCELLED = 'cancelled'
    FAILED = 'failed'
    PENDING = 'pending'
    RUNNING = 'running'
    SCHEDULED = 'scheduled'
    SUCCESS = 'success'
    TIME_OUT = 'timed_out'


class SQL_RESULT_STRATEGIES(BaseEnum):
    RENDER_JSON = 'render_json'
    SEND_TO_MAIL = 'send_to_mail'
    GENERATE_DOWNLOAD_LINK = 'generate_download_link'


class PROMOTION_PUSH_TYPES(BaseEnum):
    FB_NOTIFICATION = 'fb_notification'
    EMAIL = 'email'


class PROMOTION_PUSH_STATUSES(BaseEnum):
    FAILED = 'failed'
    PENDING = 'pending'
    PREPARING = 'preparing'
    SCHEDULED = 'scheduled'


class PROMOTION_PUSH_HISTORY_STATUSES(BaseEnum):
    REQUEST_FAILED = 'request_failed'
    FAILED = 'failed'
    RUNNING = 'running'
    SCHEDULED = 'scheduled'
    SUCCESS = 'success'
