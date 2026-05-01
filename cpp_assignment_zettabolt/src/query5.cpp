#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>
#include <cstring>
#include <iomanip>

namespace {
    std::vector<std::string> split(const std::string& s, char delimiter) {
        std::vector<std::string> tokens;
        std::string token;
        std::istringstream tokenStream(s);
        while (std::getline(tokenStream, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }

    bool loadFile(const std::string& path, std::vector<std::map<std::string, std::string>>& data, const std::vector<int>& indices, const std::vector<std::string>& keys) {
        std::ifstream file(path);
        if (!file.is_open()) {
            std::cerr << "Error: Could not open file: " << path << std::endl;
            return false;
        }
        std::string line;
        while (std::getline(file, line)) {
            auto tokens = split(line, '|');
            std::map<std::string, std::string> row;
            bool valid = true;
            for (size_t i = 0; i < indices.size(); ++i) {
                if (indices[i] < tokens.size()) {
                    row[keys[i]] = tokens[indices[i]];
                } else {
                    valid = false;
                    break;
                }
            }
            if (valid) data.push_back(row);
        }
        return true;
    }
}

bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) r_name = argv[++i];
        else if (arg == "--start_date" && i + 1 < argc) start_date = argv[++i];
        else if (arg == "--end_date" && i + 1 < argc) end_date = argv[++i];
        else if (arg == "--threads" && i + 1 < argc) num_threads = std::stoi(argv[++i]);
        else if (arg == "--table_path" && i + 1 < argc) table_path = argv[++i];
        else if (arg == "--result_path" && i + 1 < argc) result_path = argv[++i];
    }
    return !r_name.empty() && !start_date.empty() && !end_date.empty() && num_threads > 0 && !table_path.empty() && !result_path.empty();
}

bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    if (!loadFile(table_path + "/region.tbl", region_data, {0, 1}, {"regionkey", "name"})) return false;
    if (!loadFile(table_path + "/nation.tbl", nation_data, {0, 1, 2}, {"nationkey", "name", "regionkey"})) return false;
    if (!loadFile(table_path + "/supplier.tbl", supplier_data, {0, 3}, {"suppkey", "nationkey"})) return false;
    if (!loadFile(table_path + "/customer.tbl", customer_data, {0, 3}, {"custkey", "nationkey"})) return false;
    if (!loadFile(table_path + "/orders.tbl", orders_data, {0, 1, 4}, {"orderkey", "custkey", "orderdate"})) return false;
    
    std::string lineitem_path = table_path + "/lineitem.tbl";
    std::ifstream file(lineitem_path);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file: " << lineitem_path << std::endl;
        return false;
    }
    std::string line;
    while (std::getline(file, line)) {
        auto tokens = split(line, '|');
        if (tokens.size() > 6) {
             std::map<std::string, std::string> row;
             row["orderkey"] = tokens[0];
             row["suppkey"] = tokens[2];
             row["extendedprice"] = tokens[5];
             row["discount"] = tokens[6];
             lineitem_data.push_back(std::move(row));
        }
    }
    return true;
}

bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    
    std::unordered_map<std::string, std::string> region_map; 
    for (const auto& r : region_data) {
        if (r.at("name") == r_name) {
            region_map[r.at("regionkey")] = r.at("name");
        }
    }

    std::unordered_map<std::string, std::string> nation_map; 
    for (const auto& n : nation_data) {
        if (region_map.count(n.at("regionkey"))) {
            nation_map[n.at("nationkey")] = n.at("name");
        }
    }

    std::unordered_map<std::string, std::string> supplier_map; 
    for (const auto& s : supplier_data) {
        if (nation_map.count(s.at("nationkey"))) {
            supplier_map[s.at("suppkey")] = s.at("nationkey");
        }
    }

    std::unordered_map<std::string, std::string> customer_map; 
    for (const auto& c : customer_data) {
        if (nation_map.count(c.at("nationkey"))) {
            customer_map[c.at("custkey")] = c.at("nationkey");
        }
    }

    std::unordered_map<std::string, std::string> orders_map; 
    for (const auto& o : orders_data) {
        if (o.at("orderdate") >= start_date && o.at("orderdate") < end_date) {
            if (customer_map.count(o.at("custkey"))) {
                orders_map[o.at("orderkey")] = o.at("custkey");
            }
        }
    }

    std::vector<std::map<std::string, double>> thread_results(num_threads);
    std::vector<std::thread> threads;
    size_t chunk_size = lineitem_data.size() / num_threads;

    auto worker = [&](int thread_id, size_t start, size_t end) {
        for (size_t i = start; i < end; ++i) {
            const auto& l = lineitem_data[i];
            auto it_o = orders_map.find(l.at("orderkey"));
            if (it_o != orders_map.end()) {
                std::string cust_key = it_o->second;
                std::string supp_key = l.at("suppkey");
                
                auto it_s = supplier_map.find(supp_key);
                if (it_s != supplier_map.end()) {
                    std::string supp_nation = it_s->second;
                    std::string cust_nation = customer_map.at(cust_key);
                    
                    if (supp_nation == cust_nation) {
                        double extended_price = std::stod(l.at("extendedprice"));
                        double discount = std::stod(l.at("discount"));
                        double revenue = extended_price * (1.0 - discount);
                        
                        thread_results[thread_id][nation_map[supp_nation]] += revenue;
                    }
                }
            }
        }
    };

    for (int i = 0; i < num_threads; ++i) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? lineitem_data.size() : (i + 1) * chunk_size;
        threads.emplace_back(worker, i, start, end);
    }

    for (auto& t : threads) {
        t.join();
    }

    for (const auto& tr : thread_results) {
        for (const auto& pair : tr) {
            results[pair.first] += pair.second;
        }
    }

    return true;
}

bool outputResults(const std::string& result_path, const std::map<std::string, double>& results, int num_threads, double time_taken) {
    std::ofstream file(result_path);
    if (!file.is_open()) return false;
    
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    std::sort(sorted_results.begin(), sorted_results.end(), [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
        return a.second > b.second;
    });

    for (const auto& pair : sorted_results) {
        file << pair.first << " " << std::fixed << std::setprecision(4) << pair.second << "\n";
    }
    
    file << "\nThreads: " << num_threads << "\n";
    file << "Time: " << std::fixed << std::setprecision(6) << time_taken << " seconds\n";
    
    return true;
} 