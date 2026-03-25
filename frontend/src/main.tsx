import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import "./index.css";
import Layout from "./components/Layout";
import Dashboard from "./pages/Dashboard";
import ProductsList from "./pages/ProductsList";
import ProductDetail from "./pages/ProductDetail";
import ProductLineage from "./pages/ProductLineage";
import ContractsList from "./pages/ContractsList";
import ContractDetail from "./pages/ContractDetail";
import Settings from "./pages/Settings";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Dashboard />} />
          <Route path="/data-products" element={<ProductsList />} />
          <Route path="/data-products/:id" element={<ProductDetail />} />
          <Route path="/data-products/:id/lineage" element={<ProductLineage />} />
          <Route path="/data-contracts" element={<ContractsList />} />
          <Route path="/data-contracts/:id" element={<ContractDetail />} />
          <Route path="/settings" element={<Settings />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </React.StrictMode>
);
