#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <machbase_sqlcli.h>

#define SPARAM_MAX_COLUMN   4

#define ERROR_CHECK_COUNT	100000

#define RC_SUCCESS          	0
#define RC_FAILURE          	-1

#define UNUSED(aVar) do { (void)(aVar); } while(0)

#define CHECK_APPEND_RESULT(aRC, aEnv, aCon, aSTMT)             \
    if( !SQL_SUCCEEDED(aRC) )                                   \
    {                                                           \
        if( checkAppendError(aEnv, aCon, aSTMT) == RC_FAILURE ) \
        {                                                       \
            ;                                                   \
        }                                                       \
    }

typedef struct tm timestruct;

SQLHENV 	gEnv;
SQLHDBC 	gCon;

static char          gTargetIP[16];
static int           gPortNo=8086;
static unsigned long gMaxData=0;
static long gTps=0;
static char          gTable[64]="TAG";

static char *gEnvVarNames[] = { 
                          "TEST_MAX_ROWCNT",
                          "TEST_TARGET_EPS",
                          "TEST_PORT_NO",
                          "TEST_SERVER_IP",
                          NULL
};


time_t getTimeStamp();
void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
int connectDB();
void disconnectDB();
int executeDirectSQL(const char *aSQL, int aErrIgnore);
int createTable();
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt);
unsigned long appendClose(SQLHSTMT aStmt);


const char *json_array_rand =
"{\"data\" : [453.653, 731, 696.756, 397.722, 954, 8.856, 362.634, 247.639, 912.080, 894,"
"845.219, 363, 451, 674.847, 883, 755.202, 309.064, 775, 247, 511.169,"
"332.953, 414, 40, 777.847, 798, 46.609, 693, 471.749, 469, 320.298, 737.867,"
"783, 393.413, 211.180, 232, 998, 902.759, 28.812, 488.082, 860.531, 499.242,"
"376, 661, 42, 828, 407, 967.263, 347.955, 280.740, 170, 547, 683, 401,"
"406.269, 599, 975, 157, 38.372, 174, 153.238, 322, 421, 463, 295, 868.815,"
"645.142, 353.049, 344, 816, 418.503, 833, 19, 311.076, 711.093, 378.672,"
"379.798, 384.831, 72, 590, 840.655, 344, 99.324, 834.796, 706.032, 120.687,"
"576.636, 601, 373.856, 174, 809, 578.866, 528, 781.799, 96.898, 178, 207.809,"
"302.593, 613.270, 396.770, 471, 321, 167, 443, 426.993, 272.126, 183.425,"
"983.236, 806, 892, 231, 265, 263.000, 648, 542.620, 399, 709.670, 947,"
"115.468, 435, 390, 733.834, 476.316, 316.663, 780, 826.519, 52, 877, 354.507,"
"624.113, 512, 86.750, 291.691, 840.493, 724, 771, 217, 322, 425, 738, 53,"
"741, 107.611, 114, 328, 995.883, 142.649, 972, 50, 821, 759.093, 476, 20.452,"
"83, 213.726, 89.806, 853.967, 491, 12, 412.618, 967.201, 465, 830.214, 771,"
"763.229, 467, 923, 622, 483, 432.159, 338, 949.216, 181, 744.205, 323.190,"
"440, 360.313, 979, 684, 18, 627, 345, 55.560, 948.526, 350.598, 335, 39.305,"
"286.631, 430, 960, 11.628, 221, 565, 351, 532, 71, 222.961, 650, 93, 421,"
"406, 768.553, 41, 486.184, 752.227, 975.830, 842, 141, 63, 274.457, 237.987,"
"606.257, 941.754, 380, 713, 864, 831.833, 661.093, 7, 700, 813.744, 347.866,"
"191.449, 450.732, 307, 190, 108, 616.443, 297.858, 75.452, 939, 735.201,"
"473.288, 497, 937, 972.221, 751, 218, 822.025, 487.084, 742.734, 169.484,"
"929.991, 549.954, 463, 355.796, 444.159, 980.596, 865, 377, 235.076, 850.098,"
"465.581, 256, 778.951, 785.104, 578, 153, 179.636, 510.084, 413, 76, 356,"
"210.931, 449, 586, 42.366, 420.787, 296, 321.433, 470.055, 974.073, 687, 223,"
"57.038, 899, 254.431, 328, 653, 618, 113.728, 190, 734.673, 270.069, 721,"
"415.260, 946.823, 401.468, 993.935, 545.925, 153, 508, 220.972, 853.814,"
"701.973, 451.446, 53, 692, 51, 575.137, 831.633, 29, 957, 853, 706.735, 648,"
"403, 384, 548.977, 713, 198.991, 737.128, 560.291, 937, 578, 794, 303.509,"
"363.901, 910.620, 221.399, 62.223, 157.163, 263, 90.528, 38, 199, 401.310,"
"904, 505.885, 145, 89, 339, 151, 471, 305, 561.066, 200.861, 659, 798.592,"
"159, 351.816, 336.028, 839, 520.860, 60.720, 872, 817, 49.567, 801, 637.726,"
"5, 681, 2, 569.662, 174, 264.342, 783.539, 157, 382.321, 336.234, 171.591,"
"706.994, 145, 966.773, 319.623, 913, 337, 157.853, 39, 863, 912, 307,"
"920.128, 593, 423, 919, 794.997, 492.430, 389, 543.168, 453, 947.256,"
"493.526, 61, 872, 990, 974, 34, 811.932, 817.909, 890.828, 455.762, 8, 73,"
"465.648, 450, 251, 548, 349.672, 406, 855.343, 954.280, 52, 135, 614,"
"150.617, 762, 732, 797, 323, 19.030, 197, 528.042, 718, 214.712, 62, 748.417,"
"403.465, 655, 999, 660.472, 553, 642, 47, 835.353, 518.818, 600, 300, 537,"
"881, 836, 845, 392, 773, 216, 880.329, 95.302, 164.367, 776, 516, 99.667,"
"985, 370, 992.740, 846, 151, 549.601, 323, 554.868, 112.016, 528, 486.589,"
"908, 626, 874, 3, 648, 721, 244.758, 908.816, 397.956, 796.217, 163.025, 781,"
"631.421, 28.070, 903.813, 27.575, 824, 114, 367, 362, 290, 584, 449, 733,"
"350.522, 378.838, 81, 859, 563, 807.651, 833, 90, 110, 597, 954.858, 856.514,"
"201, 970, 929.526, 576, 402, 235, 491.337, 929, 794, 786, 626, 98.656,"
"702.107, 775, 622, 869.733, 215.079, 917, 418.570, 942.615, 569.594, 990.638,"
"148, 848.264, 413, 282.935, 440.343, 864.756, 266, 590, 580, 645.776, 242,"
"951, 461, 253.714, 407, 928, 423.073, 45.357, 402, 906.918, 331.157, 299,"
"269.643, 292.857, 642.515, 827, 370, 163.584, 323.675, 560, 545, 555.973,"
"942, 674, 462.885, 374.088, 519.456, 104.599, 848.240, 165.221, 578, 418,"
"52.940, 134, 58.541, 816.043, 616.519, 101, 566, 819, 371, 117.270, 551.397,"
"249.713, 130, 261.571, 300.426, 658, 145, 810.922, 917.477, 842, 466, 367,"
"356.253, 109, 439.148, 780, 987.434, 345, 665, 470.027, 393, 50.373, 128.036,"
"72.775, 172, 510, 979, 957.630, 76.150, 18.208, 526.581, 460, 475.964,"
"154.120, 467, 130.777, 965.623, 171.334, 760, 432, 504, 7, 234.720, 717.809,"
"443.372, 243, 532, 118.014, 793.808, 90.114, 969, 0.520, 25, 906, 15.635,"
"604, 731, 135, 452, 93.360, 741.171, 57, 465, 34, 181, 109, 465, 92.127,"
"409.692, 180.554, 740.088, 991, 515.488, 957, 847, 577.554, 624, 661.001,"
"191, 848, 6.110, 632, 57, 292, 387, 732, 123.457, 675.527, 358.162, 339.995,"
"936, 12, 596.985, 325.638, 253.737, 574.126, 720, 185.377, 481.004, 859.699,"
"491.370, 230, 776, 330.168, 401.220, 124, 914.951, 338, 921, 626, 884, 854,"
"531, 829, 303.356, 643, 216, 590, 559.177, 73.530, 755, 330, 707, 896,"
"691.587, 539, 253, 277, 768, 451.982, 649.869, 379, 363.266, 144, 565, 432,"
"724, 35, 390, 974, 234, 749, 280, 170.229, 39, 251.988, 813.199, 526.704,"
"858.502, 944, 402.773, 576.857, 515, 182.761, 428.233, 426.636, 89.412, 225,"
"456, 810, 114, 97.180, 985.120, 649, 321.090, 639, 851, 639.600, 439.985,"
"783.747, 359.840, 842, 529, 668.139, 658, 656, 975.682, 456.925, 620, 354,"
"683, 948, 917.874, 366, 126.559, 200, 534.645, 322.879, 596, 827.906,"
"507.515, 521.910, 158.662, 961.571, 538.610, 339.175, 307.987, 662.513,"
"726.432, 526.220, 424, 260, 701.031, 933, 338.917, 608.533, 522.400, 417,"
"140.840, 868, 209.202, 572, 165.642, 275, 940.691, 58.986, 727, 211.948, 475,"
"533.947, 288.373, 289.683, 692.374, 648.344, 76.610, 753, 222, 305.395,"
"203.955, 894, 855, 952.062, 92.376, 35.034, 839.277, 615.692, 23, 403.66,"
"604.450, 845.411, 533, 925, 142, 91, 158.686, 554.437, 824, 343.625, 744,"
"261, 91, 389, 700, 17.056, 33.568, 334.952, 338.066, 49, 593, 211, 671,"
"843.546, 177.351, 144, 813.298, 187.194, 807.446, 355.965, 672.617, 578.504,"
"276.538, 762.236, 370.627, 847, 829, 721, 693, 699.812, 241, 945, 67,"
"538.562, 898.322, 550.915, 998.471, 725, 145, 335, 136.543, 230.760, 759,"
"645.077, 342.423, 890.748, 84, 384, 984.027, 805, 448.253, 9, 725.827, 126,"
"943.461, 320, 992.191, 871, 906.342, 330.843, 899.617, 8.867, 671, 473,"
"125.955, 736, 464, 766, 683, 899, 805, 973, 953.559, 858.714, 202, 426.402,"
"632.019, 864.571, 232.350, 797, 775, 562, 208.777, 91, 984, 22, 91.403,"
"314.479, 209.452, 456, 342.855, 968, 285.946, 112.140, 538, 542, 450.405,"
"514, 531.618, 454, 52, 95.711, 107, 269, 670.900, 550.819, 564.341, 855, 816,"
"233.745, 680, 196, 739, 780.060, 756, 736.077, 344, 748, 547, 876.492, 706,"
"489.784, 353, 935.875, 42.675, 301.219, 900, 968, 357.808, 191.893, 244.314,"
"543, 357.454, 510, 562, 92, 343.075, 165.639, 575.957, 983, 70.143, 160, 963,"
"578.879, 354.235, 348, 987.177, 459, 416, 479.745, 207.972, 915.426, 998,"
"488.992, 972, 286.681, 463.385, 455.044, 693.376, 776, 483.797, 335.288,"
"76.879, 483, 409, 946.366, 229, 365, 186, 923, 495, 477.476, 688.238, 346,"
"853, 150.359, 602, 968, 553, 895.687, 577.259, 710.476, 640.732, 139.082,"
"428.439, 140.679, 413, 734, 912.090, 865, 239.708, 468.945, 998, 598, 918,"
"162, 167.532, 832, 906.409, 200.989, 118, 602, 821, 646.164, 741.888,"
"741.801, 851.163, 341, 186.716, 797.485, 793, 391, 609, 209.232, 382.853]}";

const char *json_array_seq =
"{\"data\":[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,"
"21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,"
"40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58,"
"59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77,"
"78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96,"
"97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,"
"113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127,"
"128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142,"
"143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157,"
"158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172,"
"173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187,"
"188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202,"
"203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217,"
"218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232,"
"233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247,"
"248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262,"
"263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277,"
"278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292,"
"293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307,"
"308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322,"
"323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337,"
"338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352,"
"353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367,"
"368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382,"
"383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397,"
"398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412,"
"413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427,"
"428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442,"
"443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 454, 455, 456, 457,"
"458, 459, 460, 461, 462, 463, 464, 465, 466, 467, 468, 469, 470, 471, 472,"
"473, 474, 475, 476, 477, 478, 479, 480, 481, 482, 483, 484, 485, 486, 487,"
"488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500, 501, 502,"
"503, 504, 505, 506, 507, 508, 509, 510, 511, 512, 513, 514, 515, 516, 517,"
"518, 519, 520, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532,"
"533, 534, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 546, 547,"
"548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558, 559, 560, 561, 562,"
"563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 574, 575, 576, 577,"
"578, 579, 580, 581, 582, 583, 584, 585, 586, 587, 588, 589, 590, 591, 592,"
"593, 594, 595, 596, 597, 598, 599, 600, 601, 602, 603, 604, 605, 606, 607,"
"608, 609, 610, 611, 612, 613, 614, 615, 616, 617, 618, 619, 620, 621, 622,"
"623, 624, 625, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 636, 637,"
"638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 651, 652,"
"653, 654, 655, 656, 657, 658, 659, 660, 661, 662, 663, 664, 665, 666, 667,"
"668, 669, 670, 671, 672, 673, 674, 675, 676, 677, 678, 679, 680, 681, 682,"
"683, 684, 685, 686, 687, 688, 689, 690, 691, 692, 693, 694, 695, 696, 697,"
"698, 699, 700, 701, 702, 703, 704, 705, 706, 707, 708, 709, 710, 711, 712,"
"713, 714, 715, 716, 717, 718, 719, 720, 721, 722, 723, 724, 725, 726, 727,"
"728, 729, 730, 731, 732, 733, 734, 735, 736, 737, 738, 739, 740, 741, 742,"
"743, 744, 745, 746, 747, 748, 749, 750, 751, 752, 753, 754, 755, 756, 757,"
"758, 759, 760, 761, 762, 763, 764, 765, 766, 767, 768, 769, 770, 771, 772,"
"773, 774, 775, 776, 777, 778, 779, 780, 781, 782, 783, 784, 785, 786, 787,"
"788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802,"
"803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817,"
"818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832,"
"833, 834, 835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847,"
"848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862,"
"863, 864, 865, 866, 867, 868, 869, 870, 871, 872, 873, 874, 875, 876, 877,"
"878, 879, 880, 881, 882, 883, 884, 885, 886, 887, 888, 889, 890, 891, 892,"
"893, 894, 895, 896, 897, 898, 899, 900, 901, 902, 903, 904, 905, 906, 907,"
"908, 909, 910, 911, 912, 913, 914, 915, 916, 917, 918, 919, 920, 921, 922,"
"923, 924, 925, 926, 927, 928, 929, 930, 931, 932, 933, 934, 935, 936, 937,"
"938, 939, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949, 950, 951, 952,"
"953, 954, 955, 956, 957, 958, 959, 960, 961, 962, 963, 964, 965, 966, 967,"
"968, 969, 970, 971, 972, 973, 974, 975, 976, 977, 978, 979, 980, 981, 982,"
"983, 984, 985, 986, 987, 988, 989, 990, 991, 992, 993, 994, 995, 996, 997,"
"998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010,"
"1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022,"
"1023]}";

time_t getTimeStamp()
{
    struct timeval sTimeVal;
    int            sRet;

    sRet = gettimeofday(&sTimeVal, NULL);

    if (sRet == 0)
    {
        return (time_t)(sTimeVal.tv_sec * 1000000 + sTimeVal.tv_usec);
    }
    else
    {
        return 0;
    }
}

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( aMsg != NULL )
    {
        printf("%s\n", aMsg);
    }

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS )
    {
        printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);
    }
}

int checkAppendError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS )
    {
        return RC_FAILURE;
    }

    printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);

    if( sNativeError != 9604 &&
        sNativeError != 9605 &&
        sNativeError != 9606 )
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void appendDumpError(SQLHSTMT    aStmt,
                 SQLINTEGER  aErrorCode,
                 SQLPOINTER  aErrorMessage,
                 SQLLEN      aErrorBufLen,
                 SQLPOINTER  aRowBuf,
                 SQLLEN      aRowBufLen)
{
    char       sErrMsg[1024] = {0, };
    char       sRowMsg[32 * 1024] = {0, };

    UNUSED(aStmt);

    if (aErrorMessage != NULL)
    {
        strncpy(sErrMsg, (char *)aErrorMessage, aErrorBufLen);
    }

    if (aRowBuf != NULL)
    {
        strncpy(sRowMsg, (char *)aRowBuf, aRowBufLen);
    }

    fprintf(stdout, "Append Error : [%d][%s]\n[%s]\n\n", aErrorCode, sErrMsg, sRowMsg);
}


int connectDB()
{
    char sConnStr[1024];

    if( SQLAllocEnv(&gEnv) != SQL_SUCCESS ) 
    {
        printf("SQLAllocEnv error\n");
        return RC_FAILURE;
    }

    if( SQLAllocConnect(gEnv, &gCon) != SQL_SUCCESS ) 
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf(sConnStr,"DSN=%s;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", gTargetIP, gPortNo);
    //sprintf(sConnStr,"DSN=%s;UID=IFLUX;PWD=MACHBASE;CONNTYPE=1;PORT_NO=%d", gTargetIP, gPortNo);
    printf("Connecting %s...\n", sConnStr);
    if( SQLDriverConnect( gCon, NULL,
                          (SQLCHAR *)sConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {

        printError(gEnv, gCon, NULL, "SQLDriverConnect error");

        SQLFreeConnect(gCon);
        gCon = SQL_NULL_HDBC;

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void disconnectDB()
{
    if( SQLDisconnect(gCon) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, NULL, "SQLDisconnect error");
    }

    SQLFreeConnect(gCon);
    gCon = SQL_NULL_HDBC;

    SQLFreeEnv(gEnv);
    gEnv = SQL_NULL_HENV;
}

int appendOpen(SQLHSTMT aStmt)
{
    const char *sTableName = gTable;

    if( SQLAppendOpen(aStmt, (SQLCHAR *)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendOpen Error");
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}


void appendTps(SQLHSTMT aStmt)
{
//    SQL_APPEND_PARAM sParam[SPARAM_MAX_COLUMN];
    SQL_APPEND_PARAM *sParam;
    SQLRETURN        sRC;
    unsigned long    i;
    unsigned long    sRecCount = 0;
    char	     sTagName[20];
    int               year,month,hour,min,sec,day;

    struct tm         sTm;
    long              sTime;
    long              sInterval;
    long              StartTime;
    int               sBreak = 0;

    year     =    2025;
    month    =    0;
    day      =    1;
    hour     =    0;
    min      =    0;
    sec      =    0;

    sTm.tm_year = year - 1900;
    sTm.tm_mon  = month;
    sTm.tm_mday = day;
    sTm.tm_hour = hour;
    sTm.tm_min  = min;
    sTm.tm_sec  = sec;

    sTime = mktime(&sTm);
    sTime = sTime * MACHBASE_UINT64_LITERAL(1000000000); 

    sParam = malloc(sizeof(SQL_APPEND_PARAM) * 3);
    memset(sParam, 0, sizeof(SQL_APPEND_PARAM)*3);

    sInterval = 125000;
    StartTime = getTimeStamp();
    for( i = 0; (gMaxData == 0) || sBreak == 0; i++ )
    {
        sTagName[0]=0;
        snprintf(sTagName, 20, "PLC1");
        sParam[0].mVar.mLength   = strnlen(sTagName,20);
        sParam[0].mVar.mData     = sTagName;
        sParam[1].mDateTime.mTime =  sTime;
        
        if (i % 2 == 0)
        {
            sParam[2].mVar.mLength = strlen(json_array_rand);
            sParam[2].mVar.mData =(void*) json_array_rand;
        }
        else
        {
            sParam[2].mVar.mLength = strlen(json_array_seq);
            sParam[2].mVar.mData = (void*) json_array_seq;
        }
        
        sRC = SQLAppendDataV2(aStmt, sParam);
        sRecCount++;
        CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);
        if ((gTps != 0) && (sRecCount % 10 == 0))
        {
            long usecperev = 1000000/gTps;
            long sleepus;
            long elapsed = getTimeStamp() - StartTime;
            sleepus = (usecperev * i) - elapsed;
            if (sleepus > 0)
            {
                struct timespec sleept;
                struct timespec leftt;
                sleept.tv_sec = 0;
                sleept.tv_nsec = sleepus * 1000;
                nanosleep(&sleept, &leftt);
            }
        }
        if (sRecCount >= gMaxData)
        {
            goto exit;
        }
        sTime = sTime + sInterval;
    }

/*
   printf("====================================================\n");
   printf("total time : %ld sec\n", sEndTime - sStartTime);
   printf("average tps : %f \n", ((float)gMaxData/(sEndTime - sStartTime)));
   printf("====================================================\n");
*/
exit:
    return;
}

int appendData(SQLHSTMT aStmt)
{
    appendTps(aStmt);

    return RC_SUCCESS;
}

unsigned long appendClose(SQLHSTMT aStmt)
{
    unsigned long sSuccessCount = 0;
    unsigned long sFailureCount = 0;

    if( SQLAppendClose(aStmt, (SQLBIGINT *)&sSuccessCount, (SQLBIGINT *)&sFailureCount) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    printf("success : %ld, failure : %ld\n", sSuccessCount, sFailureCount);

    return sSuccessCount;
}

int SetGlobalVariables()
{
    int i = 0;
    char        *sEnvVar;

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gMaxData = atoll(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gTps    = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gPortNo  = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    strncpy(gTargetIP, sEnvVar, sizeof(gTargetIP));
    return 0;
error:
    printf("Environment variable %s was not set!\n", gEnvVarNames[i]);
    return -1;
}

int main()
{
    SQLHSTMT    sStmt = SQL_NULL_HSTMT;

    unsigned long  sCount=0;
    time_t      sStartTime, sEndTime;

    if (SetGlobalVariables() != 0)
    {
        exit(-1);
    }
    srand(time(NULL));

    if( connectDB() == RC_SUCCESS )
    {
        printf("connectDB success\n");
    }
    else
    {
        printf("connectDB failure\n");
        goto error;
    }

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS ) 
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( appendOpen(sStmt) == RC_SUCCESS )
    {
        printf("appendOpen success\n");
    }
    else
    {
        printf("appendOpen failure\n");
        goto error;
    }

    if( SQLAppendSetErrorCallback(sStmt, appendDumpError) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAppendSetErrorCallback Error");
        goto error;
    }

    sStartTime = getTimeStamp();
    appendData(sStmt);
    sEndTime = getTimeStamp();

    sCount = appendClose(sStmt);
    if( sCount > 0 )
    {
        printf("appendClose success\n");
        printf("timegap = %ld microseconds for %ld records\n", sEndTime - sStartTime, sCount);
        printf("%.2f records/second\n",  ((double)sCount/(double)(sEndTime - sStartTime))*1000000);
    }
    else
    {
        printf("appendClose failure\n");
    }

    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;

    disconnectDB();

    return RC_SUCCESS;

error:
    if( sStmt != SQL_NULL_HSTMT )
    {
        SQLFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

    if( gCon != SQL_NULL_HDBC )
    {
        disconnectDB();
    }

    return RC_FAILURE;
}
