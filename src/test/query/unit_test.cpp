#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <cstdlib>
#include <vector>
#include <sys/stat.h>

const int NUM_TESTS = 5;
const double SCORES[] = {25, 15, 15, 15, 30};

std::string get_test_name(int index) {
    return "../src/test/query/query_sql/basic_query_test" + std::to_string(index) + ".sql";
}

std::string get_output_name(int index) {
    return "../src/test/query/query_sql/basic_query_answer" + std::to_string(index) + ".txt";
}

int extract_index(const std::string& test_file) {
    std::string index_str;
    for (char c : test_file) {
        if (isdigit(c)) {
            index_str += c;
        }
    }
    if (!index_str.empty()) {
        return std::stoi(index_str);
    } else {
        std::cerr << "Error: Could not extract index from the test file " << test_file << "." << std::endl;
        exit(1);
    }
}

void build() {
    chdir("../../../");
    struct stat info;
    if (stat("./build", &info) == 0 && (info.st_mode & S_IFDIR)) {
        chdir("./build");
        system("make rmdb -j4");
        system("make query_test -j4");
    } else {
        mkdir("./build", 0777);
        chdir("./build");
        system("cmake ..");
        system("make rmdb -j4");
        system("make query_test -j4");
    }
    chdir("..");
}

void run(const std::string& test_file) {
    chdir("./build");
    double score = 0.0;

    int test_index = extract_index(test_file);

    if (!(1 <= test_index && test_index <= NUM_TESTS)) {
        std::cerr << "Error: Invalid index. The index should be between 1 and " << NUM_TESTS << "." << std::endl;
        exit(0);
    }

    std::string test_file_name = get_test_name(test_index);
    std::string database_name = "query_test_db";

    if (access(database_name.c_str(), F_OK) == 0) {
        system(("rm -rf " + database_name).c_str());
    }

    system(("./bin/rmdb " + database_name + " &").c_str());
    sleep(3);
    int ret = system(("./bin/query_test " + test_file_name).c_str());
    if (ret != 0) {
        std::cerr << "Error. Stopping" << std::endl;
        exit(0);
    }

    std::map<std::string, int> ansDict;
    std::string standard_answer = get_output_name(test_index);
    std::ifstream hand0(standard_answer);
    std::string line;
    while (std::getline(hand0, line)) {
        line.erase(line.find_last_not_of("\n") + 1);
        if (line.empty()) continue;
        ansDict[line]++;
    }
    hand0.close();

    std::string my_answer = database_name + "/output.txt";
    std::ifstream hand1(my_answer);
    while (std::getline(hand1, line)) {
        line.erase(line.find_last_not_of("\n") + 1);
        if (line.empty()) continue;
        ansDict[line]--;
    }
    hand1.close();

    bool match = true;
    for (const auto& kv : ansDict) {
        if (kv.second != 0) {
            match = false;
            if (kv.second > 0) {
                std::cout << "In basic query test" << test_index << " Mismatch, your answer lacks items" << std::endl;
            } else {
                std::cout << "In basic query test" << test_index << " Mismatch, your answer has redundant items" << std::endl;
            }
        }
    }
    if (match) {
        score += SCORES[test_index - 1];
    }

    system("ps -ef | grep rmdb | grep -v grep | awk '{print $2}' | xargs kill -9");
    std::cout << "finish kill" << std::endl;

    if (test_index < 5) {
        system(("rm -rf ./" + database_name).c_str());
        std::cout << "finish delete database" << std::endl;
    }

    chdir("../../");
    std::cout << "Unit Test Score: " << score << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <test_file>" << std::endl;
        return 1;
    }
    build();
    run(argv[1]);
    return 0;
}