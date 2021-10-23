package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProductDaoImpl implements ProductDao {
    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            String sqlRequest = getSaveProductSql(product);
            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlRequest, Statement.RETURN_GENERATED_KEYS)) {
                preparedStatement.setString(1, product.getName());
                preparedStatement.setString(2, product.getProducer());
                preparedStatement.setBigDecimal(3, product.getPrice());
                preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
                if (product.getCreationTime() != null) {
                    preparedStatement.setTimestamp(5, Timestamp.valueOf(product.getCreationTime()));
                }
                preparedStatement.executeUpdate();
                try (ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
                    if (resultSet.next()) {
                        product.setId(resultSet.getLong("id"));
                    } else {
                        throw new DaoOperationException("Error saving product: " + product);
                    }
                }
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Error saving product: " + product);
        }
    }

    private String getSaveProductSql(Product product) {
        if (product.getCreationTime() != null) {
            return "INSERT INTO products " +
                    "(name, producer, price, expiration_date, creation_time)" +
                    " VALUES (?, ?, ?, ?, ?)";
        } else {
            return "INSERT INTO products " +
                    "(name, producer, price, expiration_date)" +
                    " VALUES (?, ?, ?, ?)";
        }
    }

    @Override
    public List<Product> findAll() {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM products")) {
                ResultSet result = preparedStatement.executeQuery();
                List<Product> resultList = getProductsFromResultSet(result);
                return resultList.isEmpty() ? Collections.emptyList() : resultList;
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Unable to find all the product. " + e.getMessage());
        }
    }

    @Override
    public Product findOne(Long id) {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM products WHERE id = ?")) {
                preparedStatement.setLong(1, id);
                ResultSet result = preparedStatement.executeQuery();
                List<Product> resultList = getProductsFromResultSet(result);
                if (resultList.isEmpty()) {
                    throw new DaoOperationException("Product with id = " + id + " does not exist");
                }
                return resultList.get(0);
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Product with id = " + id + " does not exist");
        }
    }

    private List<Product> getProductsFromResultSet(ResultSet result) throws SQLException {
        List<Product> products = new ArrayList<>();
        while (result.next()) {
            Product product = Product.builder()
                    .id(result.getLong("id"))
                    .name(result.getString("name"))
                    .producer(result.getString("producer"))
                    .price(result.getBigDecimal("price"))
                    .expirationDate(result.getTimestamp("expiration_date").toLocalDateTime().toLocalDate())
                    .creationTime(result.getTimestamp("creation_time").toLocalDateTime())
                    .build();
            products.add(product);
        }
        return products;
    }

    @Override
    public void update(Product product) {
        Long id = product.getId();
        validateId(id);

        try (Connection connection = dataSource.getConnection()) {
            String sql = getUpdateProductSql(product);
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                int paramIndex = 1;
                preparedStatement.setString(paramIndex++, product.getName());
                preparedStatement.setString(paramIndex++, product.getProducer());
                preparedStatement.setBigDecimal(paramIndex++, product.getPrice());
                preparedStatement.setDate(paramIndex++, Date.valueOf(product.getExpirationDate()));
                if (product.getCreationTime() != null) {
                    preparedStatement.setTimestamp(paramIndex++, Timestamp.valueOf(product.getCreationTime()));
                }
                preparedStatement.setLong(paramIndex, id);
                preparedStatement.execute();
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Product with id = " + id + " does not exist");
        }
    }

    private String getUpdateProductSql(Product product) {
        if (product.getCreationTime() != null) {
            return "UPDATE products " +
                    "SET name = ?, producer = ?, price = ?, expiration_date = ?, creation_time = ? " +
                    "WHERE id = ?";
        } else {
            return "UPDATE products " +
                    "SET name = ?, producer = ?, price = ?, expiration_date = ?" +
                    "WHERE id = ?";
        }
    }

    @Override
    public void remove(Product product) {
        Long id = product.getId();
        validateId(id);

        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM products WHERE id = ?")) {
                preparedStatement.setLong(1, id);
                preparedStatement.execute();
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Product with id = " + id + " does not exist");
        }
    }

    private void validateId(Long id) {
        if (id == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
        if (id <= 0) {
            throw new DaoOperationException("Product with id = " + id + " does not exist");
        }
    }

}
